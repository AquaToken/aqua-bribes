import io
from datetime import date, datetime, time, timedelta
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from unittest import mock

from django.conf import settings
from django.core.management import CommandError, call_command
from django.db import models
from django.test import TestCase, override_settings
from django.utils import timezone

import requests
from constance import config
from stellar_sdk import Asset, Claimant, ClaimPredicate, Keypair, Server, TransactionBuilder
from stellar_sdk.exceptions import BaseHorizonError

from aquarius_bribes.bribes.models import AggregatedByAssetBribe, Bribe, MarketKey
from aquarius_bribes.bribes.tasks import task_aggregate_bribes, load_market_key_details
from aquarius_bribes.rewards.eligibility import get_payable_votes
from aquarius_bribes.rewards.models import AssetHolderBalanceSnapshot, ClaimableBalance, Payout, VoteSnapshot
from aquarius_bribes.rewards.reward_payer import RewardPayer
from aquarius_bribes.rewards.tasks import (
    task_load_votes,
    task_make_claims_snapshot,
    task_make_trustees_snapshot,
    task_pay_rewards,
)
from aquarius_bribes.rewards.utils import SecuredWallet
from aquarius_bribes.rewards.votes_loader import VotesLoader
from aquarius_bribes.utils.assets import get_asset_string

random_asset_issuer = Keypair.random()
bribe_wallet = Keypair.random()

DELEGATABLE_ASSETS = [
    (Asset('upvote', random_asset_issuer.public_key), Asset('delegatedUp', random_asset_issuer.public_key))
]


@override_settings(
    REWARD_ASSET_CODE='ZZZ', REWARD_ASSET_ISSUER=random_asset_issuer.public_key,
    BRIBE_WALLET_ADDRESS=bribe_wallet.public_key, BRIBE_WALLET_SIGNER=bribe_wallet.secret,
    DELEGATABLE_ASSETS=DELEGATABLE_ASSETS,
)
class BribesTests(TestCase):
    def _create_account(self, account):
        # Fund new wallet
        response = requests.get('https://friendbot.stellar.org/?addr={}'.format(account))
        return response

    def _get_builder(self, source):
        account_info = self._load_or_create_account(source.public_key)
        base_fee = settings.BASE_FEE
        builder = TransactionBuilder(
            source_account=account_info,
            network_passphrase=settings.STELLAR_PASSPHRASE,
            base_fee=base_fee,
        )
        return builder

    def _trust_asset(self, account, asset, builder=None):
        if builder is None:
            builder = self._get_builder(account)

        builder.append_change_trust_op(
            asset=asset, source=account.public_key,
        )

        return builder

    def _payment(self, source, destination, asset, amount, builder=None):
        if builder is None:
            builder = self._get_builder(source)

        builder.append_payment_op(
            destination=destination.public_key,
            asset=asset,
            source=source.public_key,
            amount=Decimal(amount),
        )

        return builder

    def _create_wallet(self, source, destination, amount, builder=None):
        if builder is None:
            builder = self._get_builder(source)

        builder.append_create_account_op(
            destination=destination.public_key,
            source=source.public_key,
            starting_balance=Decimal(amount),
        )

        return builder

    def _send_claim(self, source, claimants, asset, amount, builder=None):
        if builder is None:
            builder = self._get_builder(source)

        builder.append_create_claimable_balance_op(
            claimants=claimants,
            asset=asset,
            source=source.public_key,
            amount=Decimal(amount),
        )

        return builder

    def _prepare_orderbook(self, amount, price):
        builder = self._get_builder(random_asset_issuer)

        builder.append_manage_buy_offer_op(
            selling_code=settings.REWARD_ASSET_CODE,
            selling_issuer=settings.REWARD_ASSET_ISSUER,
            buying_code=self.asset_xxx.code,
            buying_issuer=self.asset_xxx.issuer,
            amount=amount,
            price=price,
        )
        transaction_envelope = builder.build()
        transaction_envelope.sign(random_asset_issuer.secret)
        return self.server.submit_transaction(transaction_envelope)

    def _load_or_create_account(self, account):
        try:
            account_info = self.server.load_account(account)
        except Exception:
            self._create_account(account)
            account_info = self._load_or_create_account(account)

        return account_info

    def setUp(self):
        self.server = Server(settings.HORIZON_URL)
        self.bribe_wallet = bribe_wallet
        self.account_1 = Keypair.random()
        self.account_2 = Keypair.random()
        self.default_market_key = MarketKey(market_key=Keypair.random().public_key)
        self.default_market_key.save()
        self.asset_xxx_issuer = Keypair.random()
        self.reward_asset = Asset(code=settings.REWARD_ASSET_CODE, issuer=settings.REWARD_ASSET_ISSUER)

        self.delegation_asset = settings.DELEGATABLE_ASSETS[0][1]
        self.delegated_asset = settings.DELEGATABLE_ASSETS[0][0]

        self.asset_xxx = Asset(code='XXX', issuer=self.asset_xxx_issuer.public_key)
        self._load_or_create_account(self.asset_xxx_issuer.public_key)
        self._load_or_create_account(random_asset_issuer.public_key)

        self._load_or_create_account(self.bribe_wallet.public_key)
        self._load_or_create_account(self.account_1.public_key)
        self._load_or_create_account(self.account_2.public_key)

        builder = self._get_builder(self.asset_xxx_issuer)
        builder = self._trust_asset(self.account_1, self.asset_xxx, builder=builder)
        builder = self._trust_asset(self.account_2, self.asset_xxx, builder=builder)
        builder = self._trust_asset(self.bribe_wallet, self.reward_asset, builder=builder)
        builder = self._trust_asset(self.bribe_wallet, self.asset_xxx, builder=builder)
        builder = self._trust_asset(random_asset_issuer, self.asset_xxx, builder=builder)
        builder = self._payment(self.asset_xxx_issuer, self.account_1, self.asset_xxx, amount=1000, builder=builder)
        builder = self._payment(self.asset_xxx_issuer, self.account_2, self.asset_xxx, amount=1000, builder=builder)
        builder = self._payment(
            self.asset_xxx_issuer, self.bribe_wallet, self.asset_xxx, amount=100000000, builder=builder,
        )
        builder = self._payment(
            random_asset_issuer, self.bribe_wallet, self.reward_asset, amount=100000000, builder=builder,
        )
        transaction_envelope = builder.build()
        transaction_envelope.sign(self.asset_xxx_issuer.secret)
        transaction_envelope.sign(self.account_1.secret)
        transaction_envelope.sign(self.account_2.secret)
        transaction_envelope.sign(self.bribe_wallet.secret)
        transaction_envelope.sign(random_asset_issuer.secret)

        response = self.server.submit_transaction(transaction_envelope)
        self.assertEqual(response['successful'], True)

    def test_votes_loader(self):
        market_key = MarketKey(market_key='GBPF7NLFCYGZNHU6HS64ZGTE4YCRLAWTLFGOMFTHQ3WSUUFIGOSQFPJT')
        market_key.save()
        snapshot_time = timezone.now()
        snapshot_time = snapshot_time.replace(minute=0, second=0, microsecond=0)

        loader = VotesLoader(market_key, snapshot_time)
        loader.load_votes()

        response = requests.get(
            'https://voting-tracker.aqua.network/api/market-keys/{}/votes/?timestamp={}'.format(
                market_key, snapshot_time.strftime("%s"),
            )
        )

        self.assertEqual(VoteSnapshot.objects.count(), response.json()['count'])

    def test_votes_loader_aggregates_duplicate_delegated_votes(self):
        market_key = MarketKey(market_key=Keypair.random().public_key)
        market_key.save()
        snapshot_time = timezone.now()
        snapshot_time = snapshot_time.replace(minute=0, second=0, microsecond=0)

        delegate = Keypair.random().public_key
        delegator_1 = Keypair.random().public_key
        delegator_2 = Keypair.random().public_key

        not_unconditional = ClaimPredicate.predicate_not(
            ClaimPredicate.predicate_unconditional()
        ).to_xdr_object().to_xdr()
        unconditional = ClaimPredicate.predicate_unconditional().to_xdr_object().to_xdr()

        def create_claimable_balance(owner, asset, amount, claimants):
            balance = ClaimableBalance.objects.create(
                claimable_balance_id=Keypair.random().public_key,
                asset_code=asset.code,
                asset_issuer=asset.issuer,
                amount=Decimal(amount),
                sponsor=owner,
                paging_token='',
                last_modified_time=timezone.now(),
                last_modified_ledger=1,
                owner=owner,
            )
            for destination, raw_predicate in claimants:
                balance.claimants.create(
                    destination=destination,
                    raw_predicate=raw_predicate,
                )
            return balance

        # delegate has 300 tokens in total
        create_claimable_balance(
            owner=delegate,
            asset=self.delegation_asset,
            amount='300',
            claimants=[(market_key.market_key, unconditional)],
        )

        # each delegator has two inherited claimable balances because of extra tokens lock
        create_claimable_balance(
            owner=delegator_1,
            asset=self.delegated_asset,
            amount='120',
            claimants=[
                (delegate, not_unconditional),
                (settings.DELEGATE_MARKER, not_unconditional),
            ],
        )
        create_claimable_balance(
            owner=delegator_1,
            asset=self.delegated_asset,
            amount='60',
            claimants=[
                (delegate, not_unconditional),
                (settings.DELEGATE_MARKER, not_unconditional),
            ],
        )
        create_claimable_balance(
            owner=delegator_2,
            asset=self.delegated_asset,
            amount='80',
            claimants=[
                (delegate, not_unconditional),
                (settings.DELEGATE_MARKER, not_unconditional),
            ],
        )
        create_claimable_balance(
            owner=delegator_2,
            asset=self.delegated_asset,
            amount='40',
            claimants=[
                (delegate, not_unconditional),
                (settings.DELEGATE_MARKER, not_unconditional),
            ],
        )

        loader = VotesLoader(market_key.market_key, snapshot_time)

        # all votes come from delegate
        def vote_loading_mock(self, page, page_limit=200):
            if page and page > 1:
                return []
            return [
                {"votes_value": '300', "voting_account": delegate},
            ]

        with mock.patch.object(VotesLoader, '_get_page', new=vote_loading_mock):
            loader.load_votes()

        snapshots = VoteSnapshot.objects.filter(
            snapshot_time=snapshot_time.date(),
            market_key=market_key,
        )
        self.assertEqual(snapshots.count(), 3)

        delegate_snapshot = snapshots.get(
            voting_account=delegate,
            is_delegated=False,
            has_delegation=True,
            delegate_owner=None,
        )
        self.assertEqual(delegate_snapshot.votes_value, Decimal('300'))

        delegator_1_snapshot = snapshots.get(
            voting_account=delegator_1,
            is_delegated=True,
            has_delegation=False,
            delegate_owner=delegate,
        )
        delegator_2_snapshot = snapshots.get(
            voting_account=delegator_2,
            is_delegated=True,
            has_delegation=False,
            delegate_owner=delegate,
        )
        # delegated votes are aggregated correctly
        self.assertEqual(delegator_1_snapshot.votes_value, Decimal('180'))
        self.assertEqual(delegator_2_snapshot.votes_value, Decimal('120'))

    def test_reward_payer(self):
        market_key = MarketKey(market_key='GBPF7NLFCYGZNHU6HS64ZGTE4YCRLAWTLFGOMFTHQ3WSUUFIGOSQFPJT')
        market_key.save()

        load_market_key_details()

        snapshot_time = timezone.now()
        snapshot_time = snapshot_time.replace(minute=0, second=0, microsecond=0)

        builder = self._get_builder(self.account_1)

        votes = []
        accounts = []
        for i in range(10):
            voting_account = Keypair.random()
            accounts.append(voting_account)
            votes.append(
                VoteSnapshot(
                    votes_value=100,
                    voting_account=voting_account.public_key,
                    snapshot_time=snapshot_time,
                    market_key=market_key,
                )
            )
            builder = self._create_wallet(self.account_1, voting_account, 5, builder=builder)
            builder = self._trust_asset(voting_account, self.asset_xxx, builder=builder)
            builder = self._trust_asset(voting_account, self.reward_asset, builder=builder)
            builder = self._payment(
                self.asset_xxx_issuer, voting_account, self.asset_xxx, amount=1000, builder=builder,
            )

        transaction_envelope = builder.build()

        for i in range(10):
            transaction_envelope.sign(accounts[i].secret)

        transaction_envelope.sign(self.account_1.secret)
        transaction_envelope.sign(self.asset_xxx_issuer.secret)

        response = self.server.submit_transaction(transaction_envelope)
        self.assertEqual(response['successful'], True)

        VoteSnapshot.objects.bulk_create(votes)

        start_at = timezone.now()
        stop_at = timezone.now()

        bribe = Bribe(
            market_key=market_key,
            amount_for_bribes=100000,
            amount_aqua=config.CONVERTATION_AMOUNT,
            asset_code=self.asset_xxx.code,
            asset_issuer=self.asset_xxx.issuer,
            status=Bribe.STATUS_ACTIVE,
            amount=100000,
            created_at=timezone.now(),
            updated_at=timezone.now(),
            start_at=start_at,
            stop_at=stop_at,
        )
        bribe.save()
        task_aggregate_bribes(start_at, stop_at)
        bribe = AggregatedByAssetBribe.objects.first()

        reward_wallet = SecuredWallet(
            public_key=settings.BRIBE_WALLET_ADDRESS,
            secret=settings.BRIBE_WALLET_SIGNER,
        )

        reward_period = timedelta(hours=1)
        reward_amount = bribe.daily_amount * Decimal(reward_period.total_seconds() / (24 * 3600))
        reward_payer = RewardPayer(bribe, reward_wallet, self.reward_asset, reward_amount)
        reward_payer.pay_reward(VoteSnapshot.objects.all())

        Payout.objects.values_list('status', flat=True).distinct()
        self.assertEqual(Payout.objects.values_list('status', flat=True).distinct().count(), 1)
        self.assertEqual(Payout.objects.values_list('status', flat=True).distinct().first(), Payout.STATUS_SUCCESS)

    def test_reward_payer_with_native_asset(self):
        market_key = MarketKey(market_key='GBPF7NLFCYGZNHU6HS64ZGTE4YCRLAWTLFGOMFTHQ3WSUUFIGOSQFPJT')
        market_key.save()
        snapshot_time = timezone.now()
        snapshot_time = snapshot_time.replace(minute=0, second=0, microsecond=0)

        builder = self._get_builder(self.account_1)

        votes = []
        accounts = []
        for i in range(10):
            voting_account = Keypair.random()
            accounts.append(voting_account)
            votes.append(
                VoteSnapshot(
                    votes_value=100,
                    voting_account=voting_account.public_key,
                    snapshot_time=snapshot_time,
                    market_key=market_key,
                )
            )
            builder = self._create_wallet(self.account_1, voting_account, 5, builder=builder)
            builder = self._trust_asset(voting_account, self.asset_xxx, builder=builder)
            builder = self._trust_asset(voting_account, self.reward_asset, builder=builder)
            builder = self._payment(
                self.asset_xxx_issuer, voting_account, self.asset_xxx, amount=1000, builder=builder,
            )

        transaction_envelope = builder.build()

        for i in range(10):
            transaction_envelope.sign(accounts[i].secret)

        transaction_envelope.sign(self.account_1.secret)
        transaction_envelope.sign(self.asset_xxx_issuer.secret)

        response = self.server.submit_transaction(transaction_envelope)
        self.assertEqual(response['successful'], True)

        VoteSnapshot.objects.bulk_create(votes)

        start_at = timezone.now()
        stop_at = timezone.now()

        bribe = Bribe(
            market_key=market_key,
            amount_for_bribes=100000,
            amount_aqua=config.CONVERTATION_AMOUNT,
            asset_code=Asset.native().code,
            asset_issuer=Asset.native().issuer or '',
            status=Bribe.STATUS_ACTIVE,
            amount=100000,
            created_at=timezone.now(),
            updated_at=timezone.now(),
            start_at=start_at,
            stop_at=stop_at,
        )
        bribe.save()
        task_aggregate_bribes(start_at, stop_at)
        bribe = AggregatedByAssetBribe.objects.first()

        reward_wallet = SecuredWallet(
            public_key=settings.BRIBE_WALLET_ADDRESS,
            secret=settings.BRIBE_WALLET_SIGNER,
        )

        reward_period = timedelta(hours=1)
        reward_amount = bribe.daily_amount * Decimal(reward_period.total_seconds() / (24 * 3600))
        reward_payer = RewardPayer(bribe, reward_wallet, self.reward_asset, reward_amount)
        reward_payer.pay_reward(VoteSnapshot.objects.all())

        Payout.objects.values_list('stellar_transaction_id', flat=True).distinct()
        self.assertEqual(Payout.objects.values_list('status', flat=True).distinct().count(), 1)
        self.assertEqual(Payout.objects.values_list('status', flat=True).distinct().first(), Payout.STATUS_SUCCESS)

    def test_reward_payer_delegated(self):
        market_key = MarketKey(market_key=Keypair.random().public_key)
        market_key.save()
        snapshot_time = timezone.now()
        snapshot_time = snapshot_time.replace(minute=0, second=0, microsecond=0)

        builder = self._get_builder(self.account_1)

        start_at = timezone.now()
        bribe = AggregatedByAssetBribe(
            market_key_id=market_key,
            asset_code=self.reward_asset.code,
            asset_issuer=self.reward_asset.issuer,
            start_at=start_at - timedelta(days=1),
            stop_at=start_at + timedelta(days=1),
            total_reward_amount=100000,
        )
        bribe.save()
        task_make_trustees_snapshot()
        holders_before = AssetHolderBalanceSnapshot.objects.count()
        AssetHolderBalanceSnapshot.objects.all().delete()

        claims_before = ClaimableBalance.objects.count()

        votes = []
        accounts = []
        for i in range(10):
            voting_account = Keypair.random()
            accounts.append(voting_account)
            votes.append(
                VoteSnapshot(
                    votes_value=100,
                    voting_account=voting_account.public_key,
                    snapshot_time=snapshot_time,
                    market_key=market_key,
                )
            )
            builder = self._create_wallet(self.account_1, voting_account, 5, builder=builder)
            builder = self._trust_asset(voting_account, self.asset_xxx, builder=builder)
            builder = self._trust_asset(voting_account, self.reward_asset, builder=builder)
            builder = self._payment(
                self.asset_xxx_issuer, voting_account, self.asset_xxx, amount=1000, builder=builder,
            )

        delegation_voting_account = Keypair.random()
        accounts.append(delegation_voting_account)
        builder = self._create_wallet(self.account_1, delegation_voting_account, 5, builder=builder)
        builder = self._trust_asset(delegation_voting_account, self.delegation_asset, builder=builder)
        builder = self._payment(
            random_asset_issuer, delegation_voting_account, self.delegation_asset, amount=1000, builder=builder,
        )
        claim_after = timezone.now() + timedelta(seconds=1)
        claim_after_timestamp = int(claim_after.strftime("%s"))
        claimants = [
            Claimant(
                destination=delegation_voting_account.public_key,
                predicate=ClaimPredicate.predicate_not(
                    ClaimPredicate.predicate_before_absolute_time(claim_after_timestamp)
                ),
            ),
            Claimant(
                destination=market_key.market_key,
                predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
            ),
        ]
        builder = self._trust_asset(delegation_voting_account, self.reward_asset, builder=builder)
        builder = self._send_claim(delegation_voting_account, claimants, self.delegation_asset, amount=300, builder=builder)

        votes.append(
            VoteSnapshot(
                votes_value=300,
                voting_account=delegation_voting_account.public_key,
                snapshot_time=snapshot_time,
                market_key=market_key,
                is_delegated=False,
            )
        )

        for i in range(3):
            voting_account = Keypair.random()
            accounts.append(voting_account)

            claim_after = timezone.now() + timedelta(seconds=1)
            claim_after_timestamp = int(claim_after.strftime("%s"))
            claimants = [
                Claimant(
                    destination=voting_account.public_key,
                    predicate=ClaimPredicate.predicate_not(
                        ClaimPredicate.predicate_before_absolute_time(claim_after_timestamp)
                    ),
                ),
                Claimant(
                    destination=delegation_voting_account.public_key,
                    predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
                ),
                Claimant(
                    destination=settings.DELEGATE_MARKER,
                    predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
                )
            ]
            builder = self._create_wallet(self.account_1, voting_account, 5, builder=builder)
            builder = self._trust_asset(voting_account, self.delegated_asset, builder=builder)
            builder = self._trust_asset(voting_account, self.reward_asset, builder=builder)
            builder = self._payment(
                random_asset_issuer, voting_account, self.delegated_asset, amount=100, builder=builder,
            )
            builder = self._send_claim(voting_account, claimants, self.delegated_asset, amount=100, builder=builder)

        transaction_envelope = builder.build()

        for i in range(len(accounts)):
            transaction_envelope.sign(accounts[i].secret)

        transaction_envelope.sign(self.account_1.secret)
        transaction_envelope.sign(self.asset_xxx_issuer.secret)
        transaction_envelope.sign(random_asset_issuer.secret)

        response = self.server.submit_transaction(transaction_envelope)
        self.assertEqual(response['successful'], True)

        task_make_claims_snapshot()

        self.assertEqual(claims_before, ClaimableBalance.objects.count() - (3 + 1))  # 3 delegations + 1 delegated vote

        task_make_trustees_snapshot()
        self.assertEqual(AssetHolderBalanceSnapshot.objects.count() - holders_before, 13 + 1)  # 13 votes + bribe wallet

        def vote_loading_mock(self, page):
            if page and page > 1:
                return []
            else:
                return list(map(lambda x: {
                    "votes_value": x.votes_value,
                    "voting_account": x.voting_account
                }, votes))

        with mock.patch.object(VotesLoader, '_get_page', new=vote_loading_mock):
            task_load_votes()

        self.assertEqual(claims_before, ClaimableBalance.objects.count() - (3 + 1))  # 3 delegations + 1 delegated vote
        self.assertEqual(VoteSnapshot.objects.count(), 14)
        self.assertEqual(VoteSnapshot.objects.filter(is_delegated=True).count(), 3)

        reward_period = timedelta(hours=1)
        task_pay_rewards(reward_period=reward_period)

        reward_amount = bribe.daily_amount * Decimal(reward_period.total_seconds() / (24 * 3600))

        Payout.objects.values_list('status', flat=True).distinct()
        self.assertEqual(Payout.objects.values_list('status', flat=True).distinct().count(), 1)
        self.assertEqual(Payout.objects.values_list('status', flat=True).distinct().first(), Payout.STATUS_SUCCESS)
        self.assertEqual(Payout.objects.filter(status=Payout.STATUS_SUCCESS).count(), 13)
        print(reward_amount, Payout.objects.aggregate(total=models.Sum('reward_amount'))['total'])
        self.assertEqual(Payout.objects.aggregate(total=models.Sum('reward_amount'))['total'] - reward_amount < 0.01, True)
        self.assertEqual(Payout.objects.aggregate(total=models.Sum('reward_amount'))['total'] <= reward_amount, True)

    def test_reward_payer_delegated_with_own_votes(self):
        market_key = MarketKey(market_key='GBPF7NLFCYGZNHU6HS64ZGTE4YCRLAWTLFGOMFTHQ3WSUUFIGOSQFPJT')
        market_key.save()
        snapshot_time = timezone.now()
        snapshot_time = snapshot_time.replace(minute=0, second=0, microsecond=0)

        builder = self._get_builder(self.account_1)

        start_at = timezone.now()
        bribe = AggregatedByAssetBribe(
            market_key_id=market_key,
            asset_code=self.reward_asset.code,
            asset_issuer=self.reward_asset.issuer,
            start_at=start_at - timedelta(days=1),
            stop_at=start_at + timedelta(days=1),
            total_reward_amount=100000,
        )
        bribe.save()

        task_make_claims_snapshot()
        claims_before = ClaimableBalance.objects.count()
        ClaimableBalance.objects.all().delete()
        task_make_trustees_snapshot()
        holders_before = AssetHolderBalanceSnapshot.objects.count()
        AssetHolderBalanceSnapshot.objects.all().delete()

        votes = []
        accounts = []
        for i in range(10):
            voting_account = Keypair.random()
            accounts.append(voting_account)
            votes.append(
                VoteSnapshot(
                    votes_value=100,
                    voting_account=voting_account.public_key,
                    snapshot_time=snapshot_time,
                    market_key=market_key,
                )
            )
            builder = self._create_wallet(self.account_1, voting_account, 5, builder=builder)
            builder = self._trust_asset(voting_account, self.delegated_asset, builder=builder)
            builder = self._trust_asset(voting_account, self.reward_asset, builder=builder)
            builder = self._payment(
                random_asset_issuer, voting_account, self.delegated_asset, amount=1000, builder=builder,
            )

            claim_after = timezone.now() + timedelta(seconds=1)
            claim_after_timestamp = int(claim_after.strftime("%s"))
            claimants = [
                Claimant(
                    destination=voting_account.public_key,
                    predicate=ClaimPredicate.predicate_not(
                        ClaimPredicate.predicate_before_absolute_time(claim_after_timestamp)
                    ),
                ),
                Claimant(
                    destination=market_key.market_key,
                    predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
                ),
            ]
            builder = self._send_claim(voting_account, claimants, self.delegated_asset, amount=100, builder=builder)


        delegation_voting_account = Keypair.random()
        accounts.append(delegation_voting_account)
        builder = self._create_wallet(self.account_1, delegation_voting_account, 5, builder=builder)
        builder = self._trust_asset(delegation_voting_account, self.delegation_asset, builder=builder)
        builder = self._trust_asset(delegation_voting_account, self.delegated_asset, builder=builder)
        builder = self._trust_asset(delegation_voting_account, self.reward_asset, builder=builder)
        builder = self._payment(
            random_asset_issuer, delegation_voting_account, self.delegated_asset, amount=1000, builder=builder,
        )
        builder = self._payment(
            random_asset_issuer, delegation_voting_account, self.delegation_asset, amount=1000, builder=builder,
        )
        claim_after = timezone.now() + timedelta(seconds=1)
        claim_after_timestamp = int(claim_after.strftime("%s"))
        claimants = [
            Claimant(
                destination=delegation_voting_account.public_key,
                predicate=ClaimPredicate.predicate_not(
                    ClaimPredicate.predicate_before_absolute_time(claim_after_timestamp)
                ),
            ),
            Claimant(
                destination=market_key.market_key,
                predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
            ),
        ]
        builder = self._send_claim(delegation_voting_account, claimants, self.delegation_asset, amount=300, builder=builder)

        votes.append(
            VoteSnapshot(
                votes_value=500,
                voting_account=delegation_voting_account.public_key,
                snapshot_time=snapshot_time,
                market_key=market_key,
                is_delegated=False,
            )
        )

        claim_after = timezone.now() + timedelta(seconds=1)
        claim_after_timestamp = int(claim_after.strftime("%s"))
        claimants = [
            Claimant(
                destination=delegation_voting_account.public_key,
                predicate=ClaimPredicate.predicate_not(
                    ClaimPredicate.predicate_before_absolute_time(claim_after_timestamp)
                ),
            ),
            Claimant(
                destination=market_key.market_key,
                predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
            ),
        ]
        builder = self._send_claim(delegation_voting_account, claimants, self.delegated_asset, amount=200, builder=builder)

        for i in range(3):
            voting_account = Keypair.random()
            accounts.append(voting_account)

            claim_after = timezone.now() + timedelta(seconds=1)
            claim_after_timestamp = int(claim_after.strftime("%s"))
            claimants = [
                Claimant(
                    destination=voting_account.public_key,
                    predicate=ClaimPredicate.predicate_not(
                        ClaimPredicate.predicate_before_absolute_time(claim_after_timestamp)
                    ),
                ),
                Claimant(
                    destination=delegation_voting_account.public_key,
                    predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
                ),
                Claimant(
                    destination=settings.DELEGATE_MARKER,
                    predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
                )
            ]
            builder = self._create_wallet(self.account_1, voting_account, 5, builder=builder)
            builder = self._trust_asset(voting_account, self.delegated_asset, builder=builder)
            builder = self._trust_asset(voting_account, self.reward_asset, builder=builder)
            builder = self._payment(
                random_asset_issuer, voting_account, self.delegated_asset, amount=100, builder=builder,
            )
            builder = self._send_claim(voting_account, claimants, self.delegated_asset, amount=100, builder=builder)

        transaction_envelope = builder.build()

        for i in range(len(accounts)):
            transaction_envelope.sign(accounts[i].secret)

        transaction_envelope.sign(self.account_1.secret)
        # transaction_envelope.sign(self.asset_xxx_issuer.secret)
        transaction_envelope.sign(random_asset_issuer.secret)

        response = self.server.submit_transaction(transaction_envelope)
        self.assertEqual(response['successful'], True)

        task_make_claims_snapshot()

        self.assertEqual(claims_before, ClaimableBalance.objects.count() - (10 + 3 + 1 + 1))  # 10 votes + 3 delegations + 1 delegated vote

        task_make_trustees_snapshot()
        self.assertEqual(AssetHolderBalanceSnapshot.objects.count() - holders_before, 13 + 1)  # 13 votes + bribe wallet

        def vote_loading_mock(self, page):
            if page and page > 1:
                return []
            else:
                return list(map(lambda x: {
                    "votes_value": x.votes_value,
                    "voting_account": x.voting_account
                }, votes))

        with mock.patch.object(VotesLoader, '_get_page', new=vote_loading_mock):
            task_load_votes()

        self.assertEqual(claims_before, ClaimableBalance.objects.count() - (10 + 3 + 1 + 1))  # 10 votes + 3 delegations + 1 delegated vote
        self.assertEqual(VoteSnapshot.objects.count(), 15)
        self.assertEqual(VoteSnapshot.objects.filter(is_delegated=True).count(), 3)
        self.assertEqual(VoteSnapshot.objects.filter(has_delegation=True).count(), 1)

        reward_period = timedelta(hours=1)
        task_pay_rewards(reward_period=reward_period)

        reward_amount = bribe.daily_amount * Decimal(reward_period.total_seconds() / (24 * 3600))

        Payout.objects.values_list('status', flat=True).distinct()
        self.assertEqual(Payout.objects.values_list('status', flat=True).distinct().count(), 1)
        self.assertEqual(Payout.objects.values_list('status', flat=True).distinct().first(), Payout.STATUS_SUCCESS)
        self.assertEqual(Payout.objects.filter(status=Payout.STATUS_SUCCESS).count(), 14)
        print(reward_amount, Payout.objects.aggregate(total=models.Sum('reward_amount'))['total'])
        self.assertEqual(Payout.objects.aggregate(total=models.Sum('reward_amount'))['total'] - reward_amount < 0.01, True)
        self.assertEqual(Payout.objects.aggregate(total=models.Sum('reward_amount'))['total'] <= reward_amount, True)

    def test_reward_payer_delegated_with_not_delegated(self):
        market_key = MarketKey(market_key=Keypair.random().public_key)
        market_key.save()
        snapshot_time = timezone.now()
        snapshot_time = snapshot_time.replace(minute=0, second=0, microsecond=0)
        ClaimableBalance.objects.all().delete()

        start_at = timezone.now()
        bribe = AggregatedByAssetBribe(
            market_key_id=market_key,
            asset_code=self.reward_asset.code,
            asset_issuer=self.reward_asset.issuer,
            start_at=start_at - timedelta(days=1),
            stop_at=start_at + timedelta(days=1),
            total_reward_amount=100000,
        )
        bribe.save()

        task_make_trustees_snapshot()
        holders_before = AssetHolderBalanceSnapshot.objects.count()
        AssetHolderBalanceSnapshot.objects.all().delete()

        builder = self._get_builder(self.account_1)

        votes = []
        accounts = []
        for i in range(10):
            voting_account = Keypair.random()
            accounts.append(voting_account)
            votes.append(
                VoteSnapshot(
                    votes_value=100,
                    voting_account=voting_account.public_key,
                    snapshot_time=snapshot_time,
                    market_key=market_key,
                )
            )
            builder = self._create_wallet(self.account_1, voting_account, 5, builder=builder)
            builder = self._trust_asset(voting_account, self.asset_xxx, builder=builder)
            builder = self._trust_asset(voting_account, self.reward_asset, builder=builder)
            builder = self._payment(
                self.asset_xxx_issuer, voting_account, self.asset_xxx, amount=1000, builder=builder,
            )

        delegation_voting_account = Keypair.random()
        accounts.append(delegation_voting_account)
        builder = self._create_wallet(self.account_1, delegation_voting_account, 5, builder=builder)
        builder = self._trust_asset(delegation_voting_account, self.delegation_asset, builder=builder)
        builder = self._trust_asset(delegation_voting_account, self.reward_asset, builder=builder)
        builder = self._payment(
            random_asset_issuer, delegation_voting_account, self.delegation_asset, amount=1000, builder=builder,
        )
        claim_after = timezone.now() + timedelta(seconds=1)
        claim_after_timestamp = int(claim_after.strftime("%s"))
        claimants = [
            Claimant(
                destination=delegation_voting_account.public_key,
                predicate=ClaimPredicate.predicate_not(
                    ClaimPredicate.predicate_before_absolute_time(claim_after_timestamp)
                ),
            ),
            Claimant(
                destination=market_key.market_key,
                predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
            ),
        ]
        builder = self._send_claim(delegation_voting_account, claimants, self.delegation_asset, amount=300, builder=builder)

        votes.append(
            VoteSnapshot(
                votes_value=600,
                voting_account=delegation_voting_account.public_key,
                snapshot_time=snapshot_time,
                market_key=market_key,
                is_delegated=False,
            )
        )

        for i in range(3):
            voting_account = Keypair.random()
            accounts.append(voting_account)

            claim_after = timezone.now() + timedelta(seconds=1)
            claim_after_timestamp = int(claim_after.strftime("%s"))
            claimants = [
                Claimant(
                    destination=voting_account.public_key,
                    predicate=ClaimPredicate.predicate_not(
                        ClaimPredicate.predicate_before_absolute_time(claim_after_timestamp)
                    ),
                ),
                Claimant(
                    destination=delegation_voting_account.public_key,
                    predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
                ),
                Claimant(
                    destination=settings.DELEGATE_MARKER,
                    predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
                )
            ]
            builder = self._create_wallet(self.account_1, voting_account, 5, builder=builder)
            builder = self._trust_asset(voting_account, self.delegated_asset, builder=builder)
            builder = self._trust_asset(voting_account, self.reward_asset, builder=builder)
            builder = self._payment(
                random_asset_issuer, voting_account, self.delegated_asset, amount=100, builder=builder,
            )
            builder = self._send_claim(voting_account, claimants, self.delegated_asset, amount=100, builder=builder)

        transaction_envelope = builder.build()

        for i in range(len(accounts)):
            transaction_envelope.sign(accounts[i].secret)

        transaction_envelope.sign(self.account_1.secret)
        transaction_envelope.sign(self.asset_xxx_issuer.secret)
        transaction_envelope.sign(random_asset_issuer.secret)

        response = self.server.submit_transaction(transaction_envelope)
        self.assertEqual(response['successful'], True)

        task_make_claims_snapshot()

        self.assertEqual(ClaimableBalance.objects.filter(claimants__destination=market_key.market_key).count(), 1)
        self.assertEqual(ClaimableBalance.objects.filter(
                asset_code=self.delegated_asset.code,
                asset_issuer=self.delegated_asset.issuer,
            ).filter(claimants__destination=delegation_voting_account.public_key).count(), 3
        )

        task_make_trustees_snapshot()
        self.assertEqual(AssetHolderBalanceSnapshot.objects.count() - holders_before, 14)

        def vote_loading_mock(self, page):
            if page and page > 1:
                return []
            else:
                return list(map(lambda x: {
                    "votes_value": x.votes_value,
                    "voting_account": x.voting_account
                }, votes))

        with mock.patch.object(VotesLoader, '_get_page', new=vote_loading_mock):
            task_load_votes()

        self.assertEqual(ClaimableBalance.objects.filter(claimants__destination=market_key.market_key).count(), 1)
        self.assertEqual(ClaimableBalance.objects.filter(
                asset_code=self.delegated_asset.code,
                asset_issuer=self.delegated_asset.issuer,
            ).filter(claimants__destination=delegation_voting_account.public_key).count(), 3
        )
        self.assertEqual(VoteSnapshot.objects.count(), 15)
        self.assertEqual(VoteSnapshot.objects.filter(is_delegated=True).count(), 3)

        reward_period = timedelta(hours=1)
        task_pay_rewards(reward_period=reward_period)

        reward_amount = bribe.daily_amount * Decimal(reward_period.total_seconds() / (24 * 3600))

        Payout.objects.values_list('status', flat=True).distinct()
        self.assertEqual(Payout.objects.values_list('status', flat=True).distinct().count(), 1)
        self.assertEqual(Payout.objects.values_list('status', flat=True).distinct().first(), Payout.STATUS_SUCCESS)
        self.assertEqual(Payout.objects.filter(status=Payout.STATUS_SUCCESS).count(), 14)
        print(reward_amount, Payout.objects.aggregate(total=models.Sum('reward_amount'))['total'])
        self.assertEqual(Payout.objects.aggregate(total=models.Sum('reward_amount'))['total'] - reward_amount < 0.01, True)
        self.assertEqual(Payout.objects.aggregate(total=models.Sum('reward_amount'))['total'] <= reward_amount, True)

    def test_reward_payer_delegated_with_own_votes_2(self):
        market_key = MarketKey(market_key=Keypair.random().public_key)
        market_key.save()
        snapshot_time = timezone.now()
        snapshot_time = snapshot_time.replace(minute=0, second=0, microsecond=0)

        builder = self._get_builder(self.account_1)

        start_at = timezone.now()
        bribe = AggregatedByAssetBribe(
            market_key_id=market_key,
            asset_code=self.reward_asset.code,
            asset_issuer=self.reward_asset.issuer,
            start_at=start_at - timedelta(days=1),
            stop_at=start_at + timedelta(days=1),
            total_reward_amount=100000,
        )
        bribe.save()

        task_make_claims_snapshot()
        claims_before = ClaimableBalance.objects.count()
        ClaimableBalance.objects.all().delete()
        task_make_trustees_snapshot()
        holders_before = AssetHolderBalanceSnapshot.objects.count()
        AssetHolderBalanceSnapshot.objects.all().delete()

        votes = []
        accounts = []
        for i in range(10):
            voting_account = Keypair.random()
            accounts.append(voting_account)
            votes.append(
                VoteSnapshot(
                    votes_value=100,
                    voting_account=voting_account.public_key,
                    snapshot_time=snapshot_time,
                    market_key=market_key,
                )
            )
            builder = self._create_wallet(self.account_1, voting_account, 5, builder=builder)
            builder = self._trust_asset(voting_account, self.delegated_asset, builder=builder)
            builder = self._trust_asset(voting_account, self.reward_asset, builder=builder)
            builder = self._payment(
                random_asset_issuer, voting_account, self.delegated_asset, amount=1000, builder=builder,
            )

            claim_after = timezone.now() + timedelta(seconds=1)
            claim_after_timestamp = int(claim_after.strftime("%s"))
            claimants = [
                Claimant(
                    destination=voting_account.public_key,
                    predicate=ClaimPredicate.predicate_not(
                        ClaimPredicate.predicate_before_absolute_time(claim_after_timestamp)
                    ),
                ),
                Claimant(
                    destination=market_key.market_key,
                    predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
                ),
            ]
            builder = self._send_claim(voting_account, claimants, self.delegated_asset, amount=100, builder=builder)


        delegation_voting_account = Keypair.random()
        print(delegation_voting_account.public_key)
        accounts.append(delegation_voting_account)
        builder = self._create_wallet(self.account_1, delegation_voting_account, 5, builder=builder)
        builder = self._trust_asset(delegation_voting_account, self.delegation_asset, builder=builder)
        builder = self._trust_asset(delegation_voting_account, self.delegated_asset, builder=builder)
        builder = self._trust_asset(delegation_voting_account, self.reward_asset, builder=builder)
        builder = self._payment(
            random_asset_issuer, delegation_voting_account, self.delegated_asset, amount=1000, builder=builder,
        )
        builder = self._payment(
            random_asset_issuer, delegation_voting_account, self.delegation_asset, amount=1000, builder=builder,
        )
        claim_after = timezone.now() + timedelta(seconds=1)
        claim_after_timestamp = int(claim_after.strftime("%s"))
        claimants = [
            Claimant(
                destination=delegation_voting_account.public_key,
                predicate=ClaimPredicate.predicate_not(
                    ClaimPredicate.predicate_before_absolute_time(claim_after_timestamp)
                ),
            ),
            Claimant(
                destination=market_key.market_key,
                predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
            ),
        ]
        delegation_voting_account_delegated_votes = 30
        builder = self._send_claim(delegation_voting_account, claimants, self.delegation_asset, amount=delegation_voting_account_delegated_votes, builder=builder)

        claim_after = timezone.now() + timedelta(seconds=1)
        claim_after_timestamp = int(claim_after.strftime("%s"))
        claimants = [
            Claimant(
                destination=delegation_voting_account.public_key,
                predicate=ClaimPredicate.predicate_not(
                    ClaimPredicate.predicate_before_absolute_time(claim_after_timestamp)
                ),
            ),
            Claimant(
                destination=market_key.market_key,
                predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
            ),
        ]
        delegation_voting_account_own_votes = 200
        builder = self._send_claim(delegation_voting_account, claimants, self.delegated_asset, amount=delegation_voting_account_own_votes, builder=builder)

        votes.append(
            VoteSnapshot(
                votes_value=delegation_voting_account_delegated_votes + delegation_voting_account_own_votes,
                voting_account=delegation_voting_account.public_key,
                snapshot_time=snapshot_time,
                market_key=market_key,
                is_delegated=False,
            )
        )

        for i in range(3):
            voting_account = Keypair.random()
            accounts.append(voting_account)

            claim_after = timezone.now() + timedelta(seconds=1)
            claim_after_timestamp = int(claim_after.strftime("%s"))
            claimants = [
                Claimant(
                    destination=voting_account.public_key,
                    predicate=ClaimPredicate.predicate_not(
                        ClaimPredicate.predicate_before_absolute_time(claim_after_timestamp)
                    ),
                ),
                Claimant(
                    destination=delegation_voting_account.public_key,
                    predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
                ),
                Claimant(
                    destination=settings.DELEGATE_MARKER,
                    predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
                )
            ]
            builder = self._create_wallet(self.account_1, voting_account, 5, builder=builder)
            builder = self._trust_asset(voting_account, self.delegated_asset, builder=builder)
            builder = self._trust_asset(voting_account, self.reward_asset, builder=builder)
            builder = self._payment(
                random_asset_issuer, voting_account, self.delegated_asset, amount=100, builder=builder,
            )
            builder = self._send_claim(voting_account, claimants, self.delegated_asset, amount=100, builder=builder)

        transaction_envelope = builder.build()

        for i in range(len(accounts)):
            transaction_envelope.sign(accounts[i].secret)

        transaction_envelope.sign(self.account_1.secret)
        # transaction_envelope.sign(self.asset_xxx_issuer.secret)
        transaction_envelope.sign(random_asset_issuer.secret)

        response = self.server.submit_transaction(transaction_envelope)
        self.assertEqual(response['successful'], True)

        task_make_claims_snapshot()

        self.assertEqual(claims_before, ClaimableBalance.objects.count() - (10 + 3 + 1 + 1))  # 10 votes + 3 delegations + 1 delegated vote

        task_make_trustees_snapshot()
        self.assertEqual(AssetHolderBalanceSnapshot.objects.count() - holders_before, 13 + 1)  # 13 votes + bribe wallet

        def vote_loading_mock(self, page):
            if page and page > 1:
                return []
            else:
                return list(map(lambda x: {
                    "votes_value": x.votes_value,
                    "voting_account": x.voting_account
                }, votes))

        with mock.patch.object(VotesLoader, '_get_page', new=vote_loading_mock):
            task_load_votes()

        self.assertEqual(claims_before, ClaimableBalance.objects.count() - (10 + 3 + 1 + 1))  # 10 votes + 3 delegations + 1 delegated vote
        self.assertEqual(VoteSnapshot.objects.count(), 15)
        self.assertEqual(VoteSnapshot.objects.filter(is_delegated=True).count(), 3)
        self.assertEqual(VoteSnapshot.objects.filter(has_delegation=True).count(), 1)

        reward_period = timedelta(hours=1)
        task_pay_rewards(reward_period=reward_period)

        reward_amount = bribe.daily_amount * Decimal(reward_period.total_seconds() / (24 * 3600))

        Payout.objects.values_list('status', flat=True).distinct()
        self.assertEqual(Payout.objects.values_list('status', flat=True).distinct().count(), 1)
        self.assertEqual(Payout.objects.values_list('status', flat=True).distinct().first(), Payout.STATUS_SUCCESS)
        self.assertEqual(Payout.objects.filter(status=Payout.STATUS_SUCCESS).count(), 14)
        print(reward_amount, Payout.objects.aggregate(total=models.Sum('reward_amount'))['total'])
        self.assertEqual(Payout.objects.aggregate(total=models.Sum('reward_amount'))['total'] - reward_amount < 0.01, True)
        self.assertEqual(Payout.objects.aggregate(total=models.Sum('reward_amount'))['total'] <= reward_amount, True)


    def test_reward_payer_delegated_with_own_votes_two_markets(self):
        market_key = MarketKey(market_key=Keypair.random().public_key)
        market_key.save()
        snapshot_time = timezone.now()
        snapshot_time = snapshot_time.replace(minute=0, second=0, microsecond=0)

        builder = self._get_builder(self.account_1)

        start_at = timezone.now()
        bribe = AggregatedByAssetBribe(
            market_key_id=market_key,
            asset_code=self.reward_asset.code,
            asset_issuer=self.reward_asset.issuer,
            start_at=start_at - timedelta(days=1),
            stop_at=start_at + timedelta(days=1),
            total_reward_amount=100000,
        )
        bribe.save()

        task_make_claims_snapshot()
        claims_before = ClaimableBalance.objects.count()
        ClaimableBalance.objects.all().delete()
        task_make_trustees_snapshot()
        holders_before = AssetHolderBalanceSnapshot.objects.count()
        AssetHolderBalanceSnapshot.objects.all().delete()

        votes = []
        accounts = []
        for i in range(10):
            voting_account = Keypair.random()
            accounts.append(voting_account)
            votes.append(
                VoteSnapshot(
                    votes_value=100,
                    voting_account=voting_account.public_key,
                    snapshot_time=snapshot_time,
                    market_key=market_key,
                )
            )
            builder = self._create_wallet(self.account_1, voting_account, 5, builder=builder)
            builder = self._trust_asset(voting_account, self.delegated_asset, builder=builder)
            builder = self._trust_asset(voting_account, self.reward_asset, builder=builder)
            builder = self._payment(
                random_asset_issuer, voting_account, self.delegated_asset, amount=1000, builder=builder,
            )

            claim_after = timezone.now() + timedelta(seconds=1)
            claim_after_timestamp = int(claim_after.strftime("%s"))
            claimants = [
                Claimant(
                    destination=voting_account.public_key,
                    predicate=ClaimPredicate.predicate_not(
                        ClaimPredicate.predicate_before_absolute_time(claim_after_timestamp)
                    ),
                ),
                Claimant(
                    destination=market_key.market_key,
                    predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
                ),
            ]
            builder = self._send_claim(voting_account, claimants, self.delegated_asset, amount=100, builder=builder)


        delegation_voting_account = Keypair.random()
        print(delegation_voting_account.public_key)
        accounts.append(delegation_voting_account)
        builder = self._create_wallet(self.account_1, delegation_voting_account, 10, builder=builder)
        builder = self._trust_asset(delegation_voting_account, self.delegation_asset, builder=builder)
        builder = self._trust_asset(delegation_voting_account, self.delegated_asset, builder=builder)
        builder = self._trust_asset(delegation_voting_account, self.reward_asset, builder=builder)
        builder = self._payment(
            random_asset_issuer, delegation_voting_account, self.delegated_asset, amount=1000, builder=builder,
        )
        builder = self._payment(
            random_asset_issuer, delegation_voting_account, self.delegation_asset, amount=1000, builder=builder,
        )
        claim_after = timezone.now() + timedelta(seconds=1)
        claim_after_timestamp = int(claim_after.strftime("%s"))
        claimants = [
            Claimant(
                destination=delegation_voting_account.public_key,
                predicate=ClaimPredicate.predicate_not(
                    ClaimPredicate.predicate_before_absolute_time(claim_after_timestamp)
                ),
            ),
            Claimant(
                destination=market_key.market_key,
                predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
            ),
        ]
        delegation_voting_account_delegated_votes = 30
        builder = self._send_claim(delegation_voting_account, claimants, self.delegation_asset, amount=delegation_voting_account_delegated_votes, builder=builder)

        claim_after = timezone.now() + timedelta(seconds=1)
        claim_after_timestamp = int(claim_after.strftime("%s"))
        claimants = [
            Claimant(
                destination=delegation_voting_account.public_key,
                predicate=ClaimPredicate.predicate_not(
                    ClaimPredicate.predicate_before_absolute_time(claim_after_timestamp)
                ),
            ),
            Claimant(
                destination=market_key.market_key,
                predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
            ),
        ]
        delegation_voting_account_own_votes = 200
        builder = self._send_claim(delegation_voting_account, claimants, self.delegated_asset, amount=delegation_voting_account_own_votes, builder=builder)

        votes.append(
            VoteSnapshot(
                votes_value=delegation_voting_account_delegated_votes + delegation_voting_account_own_votes,
                voting_account=delegation_voting_account.public_key,
                snapshot_time=snapshot_time,
                market_key=market_key,
                is_delegated=False,
            )
        )

        for i in range(3):
            voting_account = Keypair.random()
            accounts.append(voting_account)

            claim_after = timezone.now() + timedelta(seconds=1)
            claim_after_timestamp = int(claim_after.strftime("%s"))
            claimants = [
                Claimant(
                    destination=voting_account.public_key,
                    predicate=ClaimPredicate.predicate_not(
                        ClaimPredicate.predicate_before_absolute_time(claim_after_timestamp)
                    ),
                ),
                Claimant(
                    destination=delegation_voting_account.public_key,
                    predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
                ),
                Claimant(
                    destination=settings.DELEGATE_MARKER,
                    predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
                )
            ]
            builder = self._create_wallet(self.account_1, voting_account, 5, builder=builder)
            builder = self._trust_asset(voting_account, self.delegated_asset, builder=builder)
            builder = self._trust_asset(voting_account, self.reward_asset, builder=builder)
            builder = self._payment(
                random_asset_issuer, voting_account, self.delegated_asset, amount=100, builder=builder,
            )
            builder = self._send_claim(voting_account, claimants, self.delegated_asset, amount=100, builder=builder)

        builder_2 = self._get_builder(self.account_2)
        market_key_2 = MarketKey(market_key=Keypair.random().public_key)
        market_key_2.save()
        snapshot_time = timezone.now()
        snapshot_time = snapshot_time.replace(minute=0, second=0, microsecond=0)

        start_at = timezone.now()
        bribe_2 = AggregatedByAssetBribe(
            market_key_id=market_key_2,
            asset_code=self.reward_asset.code,
            asset_issuer=self.reward_asset.issuer,
            start_at=start_at - timedelta(days=1),
            stop_at=start_at + timedelta(days=1),
            total_reward_amount=100000,
        )
        bribe_2.save()

        task_make_claims_snapshot()
        claims_before = ClaimableBalance.objects.count()
        ClaimableBalance.objects.all().delete()
        task_make_trustees_snapshot()
        holders_before = AssetHolderBalanceSnapshot.objects.count()
        AssetHolderBalanceSnapshot.objects.all().delete()

        votes_2 = []
        accounts_2 = []
        for i in range(10):
            voting_account = Keypair.random()
            accounts_2.append(voting_account)
            votes_2.append(
                VoteSnapshot(
                    votes_value=100,
                    voting_account=voting_account.public_key,
                    snapshot_time=snapshot_time,
                    market_key=market_key_2,
                )
            )
            builder_2 = self._create_wallet(self.account_2, voting_account, 5, builder=builder_2)
            builder_2 = self._trust_asset(voting_account, self.delegated_asset, builder=builder_2)
            builder_2 = self._trust_asset(voting_account, self.reward_asset, builder=builder_2)
            builder_2 = self._payment(
                random_asset_issuer, voting_account, self.delegated_asset, amount=1000, builder=builder_2,
            )

            claim_after = timezone.now() + timedelta(seconds=1)
            claim_after_timestamp = int(claim_after.strftime("%s"))
            claimants = [
                Claimant(
                    destination=voting_account.public_key,
                    predicate=ClaimPredicate.predicate_not(
                        ClaimPredicate.predicate_before_absolute_time(claim_after_timestamp)
                    ),
                ),
                Claimant(
                    destination=market_key_2.market_key,
                    predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
                ),
            ]
            builder_2 = self._send_claim(voting_account, claimants, self.delegated_asset, amount=100, builder=builder_2)


        claim_after = timezone.now() + timedelta(seconds=1)
        claim_after_timestamp = int(claim_after.strftime("%s"))
        claimants = [
            Claimant(
                destination=delegation_voting_account.public_key,
                predicate=ClaimPredicate.predicate_not(
                    ClaimPredicate.predicate_before_absolute_time(claim_after_timestamp)
                ),
            ),
            Claimant(
                destination=market_key_2.market_key,
                predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
            ),
        ]
        delegation_voting_account_delegated_votes_2 = 50
        builder_2 = self._send_claim(delegation_voting_account, claimants, self.delegation_asset, amount=delegation_voting_account_delegated_votes_2, builder=builder_2)

        claim_after = timezone.now() + timedelta(seconds=1)
        claim_after_timestamp = int(claim_after.strftime("%s"))
        claimants = [
            Claimant(
                destination=delegation_voting_account.public_key,
                predicate=ClaimPredicate.predicate_not(
                    ClaimPredicate.predicate_before_absolute_time(claim_after_timestamp)
                ),
            ),
            Claimant(
                destination=market_key_2.market_key,
                predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
            ),
        ]
        delegation_voting_account_own_votes_2 = 170
        builder_2 = self._send_claim(delegation_voting_account, claimants, self.delegated_asset, amount=delegation_voting_account_own_votes_2, builder=builder_2)

        votes_2.append(
            VoteSnapshot(
                votes_value=delegation_voting_account_delegated_votes_2 + delegation_voting_account_own_votes_2,
                voting_account=delegation_voting_account.public_key,
                snapshot_time=snapshot_time,
                market_key=market_key_2,
                is_delegated=False,
            )
        )

        for i in range(3):
            voting_account = Keypair.random()
            accounts_2.append(voting_account)

            claim_after = timezone.now() + timedelta(seconds=1)
            claim_after_timestamp = int(claim_after.strftime("%s"))
            claimants = [
                Claimant(
                    destination=voting_account.public_key,
                    predicate=ClaimPredicate.predicate_not(
                        ClaimPredicate.predicate_before_absolute_time(claim_after_timestamp)
                    ),
                ),
                Claimant(
                    destination=delegation_voting_account.public_key,
                    predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
                ),
                Claimant(
                    destination=settings.DELEGATE_MARKER,
                    predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
                )
            ]
            builder_2 = self._create_wallet(self.account_2, voting_account, 5, builder=builder_2)
            builder_2 = self._trust_asset(voting_account, self.delegated_asset, builder=builder_2)
            builder_2 = self._trust_asset(voting_account, self.reward_asset, builder=builder_2)
            builder_2 = self._payment(
                random_asset_issuer, voting_account, self.delegated_asset, amount=100, builder=builder_2,
            )
            builder_2 = self._send_claim(voting_account, claimants, self.delegated_asset, amount=100, builder=builder_2)

        transaction_envelope = builder.build()
        transaction_envelope_2 = builder_2.build()

        for i in range(len(accounts)):
            transaction_envelope.sign(accounts[i].secret)
        for i in range(len(accounts_2)):
            transaction_envelope_2.sign(accounts_2[i].secret)

        transaction_envelope.sign(self.account_1.secret)
        transaction_envelope_2.sign(self.account_2.secret)
        transaction_envelope_2.sign(delegation_voting_account.secret)
        # transaction_envelope.sign(self.asset_xxx_issuer.secret)
        transaction_envelope.sign(random_asset_issuer.secret)
        transaction_envelope_2.sign(random_asset_issuer.secret)

        response = self.server.submit_transaction(transaction_envelope)
        self.assertEqual(response['successful'], True)
        response = self.server.submit_transaction(transaction_envelope_2)
        self.assertEqual(response['successful'], True)

        task_make_claims_snapshot()

        self.assertEqual(claims_before, ClaimableBalance.objects.count() - (10 + 3 + 1 + 1) * 2)  # 10 votes + 3 delegations + 1 delegated vote

        task_make_trustees_snapshot()
        self.assertEqual(AssetHolderBalanceSnapshot.objects.count() - holders_before, 26 + 1)  # 13 votes + bribe wallet

        def vote_loading_mock(self, page):
            if page and page > 1:
                return []
            else:
                if self.market_key == market_key.market_key:
                    return list(map(lambda x: {
                        "votes_value": x.votes_value,
                        "voting_account": x.voting_account
                    }, votes))
                elif self.market_key == market_key_2.market_key:
                    return list(map(lambda x: {
                        "votes_value": x.votes_value,
                        "voting_account": x.voting_account
                    }, votes_2))

        with mock.patch.object(VotesLoader, '_get_page', new=vote_loading_mock):
            task_load_votes()

        self.assertEqual(claims_before, ClaimableBalance.objects.count() - (10 + 3 + 1 + 1) * 2)  # 10 votes + 3 delegations + 1 delegated vote
        self.assertEqual(VoteSnapshot.objects.count(), 36)  # 20 true votes + 2 votes from delegate owner + 2 delegations + 6 * 2 (6 delegators and 2 markets)
        self.assertEqual(VoteSnapshot.objects.filter(is_delegated=True).count(), 12)  # 6 * 2 (6 delegators and 2 markets)
        self.assertEqual(VoteSnapshot.objects.filter(has_delegation=True).count(), 2)

        reward_period = timedelta(hours=1)
        task_pay_rewards(reward_period=reward_period)

        reward_amount = bribe.daily_amount * Decimal(reward_period.total_seconds() / (24 * 3600))
        reward_amount_2 = bribe_2.daily_amount * Decimal(reward_period.total_seconds() / (24 * 3600))

        Payout.objects.values_list('status', flat=True).distinct()
        self.assertEqual(Payout.objects.values_list('status', flat=True).distinct().count(), 1)
        self.assertEqual(Payout.objects.values_list('status', flat=True).distinct().first(), Payout.STATUS_SUCCESS)
        self.assertEqual(Payout.objects.filter(status=Payout.STATUS_SUCCESS).count(), 34)
        self.assertEqual(Payout.objects.filter(status=Payout.STATUS_SUCCESS).count(), VoteSnapshot.objects.exclude(has_delegation=True).count())
        self.assertEqual(
            Payout.objects.filter(
                status=Payout.STATUS_SUCCESS,
                vote_snapshot__voting_account=delegation_voting_account.public_key,
                vote_snapshot__market_key=market_key_2.market_key,
            ).first().reward_amount - reward_amount_2 * (delegation_voting_account_own_votes_2) / sum(map(lambda x: x.votes_value, votes_2)) < 0.01, True
        )
        self.assertEqual(
            sum(
                map(
                    lambda x: x.reward_amount,
                    Payout.objects.filter(
                        status=Payout.STATUS_SUCCESS,
                        vote_snapshot__voting_account__in=list(map(lambda x: x.public_key, accounts_2[-3:])),
                        vote_snapshot__market_key=market_key_2.market_key,
                    ),
                ),
            ) - reward_amount_2 * (delegation_voting_account_delegated_votes_2) / sum(map(lambda x: x.votes_value, votes_2)) < 0.01, True,
        )
        print(reward_amount + reward_amount_2, Payout.objects.aggregate(total=models.Sum('reward_amount'))['total'])
        self.assertEqual(Payout.objects.aggregate(total=models.Sum('reward_amount'))['total'] - reward_amount - reward_amount_2 < 0.01, True)
        self.assertEqual(Payout.objects.aggregate(total=models.Sum('reward_amount'))['total'] <= reward_amount + reward_amount_2, True)


class ReconcileAndMonitoringTests(TestCase):
    def tearDown(self):
        from django.core.cache import cache as _cache
        from aquarius_bribes.rewards.tasks import (
            LOAD_VOTES_TASK_ACTIVE_KEY,
            LOAD_TRUSTORS_TASK_ACTIVE_KEY,
            PAY_REWARDS_FIX_DB_ACTIVE_KEY,
            PAY_REWARDS_TASK_ACTIVE_KEY,
        )

        for k in (
            LOAD_VOTES_TASK_ACTIVE_KEY,
            LOAD_TRUSTORS_TASK_ACTIVE_KEY,
            PAY_REWARDS_FIX_DB_ACTIVE_KEY,
            PAY_REWARDS_TASK_ACTIVE_KEY,
        ):
            _cache.delete(k)

    def _make_market(self, key=None, asset1=None, asset2=None):
        from stellar_sdk import Keypair

        market = MarketKey.objects.create(
            market_key=key or Keypair.random().public_key,
        )
        if asset1 is not None or asset2 is not None:
            if asset1 is not None:
                market.raw_asset1 = asset1
            if asset2 is not None:
                market.raw_asset2 = asset2
            market.save(update_fields=["raw_asset1", "raw_asset2"])
        return market

    def _make_bribe(
        self,
        market,
        asset_code="native",
        asset_issuer="",
        start=None,
        stop=None,
        total=Decimal("700"),
    ):
        start = start or timezone.now() - timedelta(days=1)
        stop = stop or start + timedelta(days=7)
        return AggregatedByAssetBribe.objects.create(
            market_key=market,
            asset_code=asset_code,
            asset_issuer=asset_issuer,
            start_at=start,
            stop_at=stop,
            total_reward_amount=total,
        )

    def _make_day_bribe(self, market, snapshot_date, **kwargs):
        # _make_bribe with start/stop defaulted to the [D-1, D+2] window
        # spanning snapshot_date — the shape used by every reconcile test
        # that needs a bribe active on a specific UTC day.
        kwargs.setdefault(
            "start", self._at(snapshot_date, 0) - timedelta(days=1),
        )
        kwargs.setdefault(
            "stop", self._at(snapshot_date, 0) + timedelta(days=2),
        )
        return self._make_bribe(market, **kwargs)

    def _make_payout(
        self,
        bribe,
        vote,
        tx_hash,
        amount,
        status=Payout.STATUS_SUCCESS,
        asset_code=None,
        asset_issuer="",
    ):
        # Defaults to a SUCCESS Payout on the native asset; callers
        # override asset_code/asset_issuer for non-native bribes and
        # pass status=Payout.STATUS_FAILED for the drift paths.
        return Payout.objects.create(
            bribe=bribe,
            vote_snapshot=vote,
            stellar_transaction_id=tx_hash,
            status=status,
            reward_amount=Decimal(amount)
            if not isinstance(amount, Decimal)
            else amount,
            asset_code=asset_code or Asset.native().code,
            asset_issuer=asset_issuer,
        )

    def _make_vote(
        self, market, account, snapshot_date, votes_value, has_delegation=False
    ):
        return VoteSnapshot.objects.create(
            market_key=market,
            voting_account=account,
            votes_value=Decimal(votes_value),
            snapshot_time=snapshot_date,
            has_delegation=has_delegation,
        )

    def _make_holder(self, account, asset_code, asset_issuer, created_at):
        snap = AssetHolderBalanceSnapshot.objects.create(
            account=account,
            asset_code=asset_code,
            asset_issuer=asset_issuer,
            balance=Decimal("1"),
        )
        AssetHolderBalanceSnapshot.objects.filter(pk=snap.pk).update(
            created_at=created_at
        )
        return snap

    def _at(self, snapshot_date, hour):
        return timezone.make_aware(datetime.combine(snapshot_date, time(hour=hour)))

    def _make_dust_votes(self, market, snapshot_date):
        regular_votes = [
            self._make_vote(
                market, Keypair.random().public_key, snapshot_date, "250000"
            )
            for _ in range(4)
        ]
        dust_vote = self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "1"
        )
        return regular_votes, dust_vote

    def _chain_record(
        self,
        bribe_wallet,
        to,
        asset_code,
        asset_issuer,
        amount,
        tx_hash,
        memo,
        created_at_str,
        paging_token=None,
    ):
        return {
            "id": paging_token or tx_hash,
            "paging_token": paging_token or tx_hash,
            "type": "payment",
            "created_at": created_at_str,
            "transaction_hash": tx_hash,
            "from": bribe_wallet,
            "to": to,
            "asset_type": "native"
            if not asset_code or asset_code == "XLM"
            else "credit_alphanum4",
            "asset_code": asset_code,
            "asset_issuer": asset_issuer,
            "amount": str(amount),
            "transaction": {
                "successful": True,
                "memo_type": "text",
                "memo": memo,
                "hash": tx_hash,
                "source_account": bribe_wallet,
            },
        }

    def _mock_reconcile_server(self, *pages):
        server = mock.MagicMock()
        builder = server.payments.return_value.for_account.return_value.include_failed.return_value
        builder.join.return_value = builder
        builder.limit.return_value = builder
        builder.order.return_value = builder
        builder.cursor.return_value = builder
        builder.call.side_effect = [{"_embedded": {"records": page}} for page in pages]
        return server, builder

    def _reconcile(self, *args, **kwargs):
        from aquarius_bribes.rewards.reconcile import reconcile_bribe_payouts

        return reconcile_bribe_payouts(*args, **kwargs)

    def _old_payable_vote_ids(self, bribe, snapshot_date, reward_amount):
        votes = VoteSnapshot.objects.filter(
            snapshot_time=snapshot_date,
            market_key=bribe.market_key,
        )

        if bribe.asset.type != Asset.native().type:
            votes = votes.filter(
                voting_account__in=AssetHolderBalanceSnapshot.objects.filter(
                    created_at__gte=snapshot_date,
                    created_at__lt=snapshot_date + timedelta(days=1),
                    asset_code=bribe.asset_code,
                    asset_issuer=bribe.asset_issuer,
                ).values_list("account"),
            )

        votes = votes.exclude(has_delegation=True)

        total_votes = votes.aggregate(total=models.Sum("votes_value"))["total"]
        if reward_amount is not None and total_votes and total_votes > 0:
            min_votes_value = Decimal(
                Decimal("0.0000001") * total_votes / Decimal(reward_amount),
            ).quantize(Decimal("0.0000001"), rounding=ROUND_UP)
            votes = votes.filter(votes_value__gte=min_votes_value)

        return set(votes.values_list("id", flat=True))

    def test_eligibility_parity_with_task_pay_rewards(self):
        snapshot_date = timezone.now().date()
        reward_amount = Decimal("0.0001000")

        native_market = self._make_market()
        native_bribe = self._make_bribe(
            native_market,
            asset_code=Asset.native().code,
            asset_issuer="",
            total=Decimal("0.0007000"),
        )
        self._make_vote(
            native_market, Keypair.random().public_key, snapshot_date, "250000"
        )
        self._make_vote(
            native_market, Keypair.random().public_key, snapshot_date, "250000"
        )
        self._make_vote(
            native_market, Keypair.random().public_key, snapshot_date, "250000"
        )
        self._make_vote(native_market, Keypair.random().public_key, snapshot_date, "1")
        self._make_vote(
            native_market,
            Keypair.random().public_key,
            snapshot_date,
            "250000",
            has_delegation=True,
        )

        expected_native_ids = self._old_payable_vote_ids(
            native_bribe, snapshot_date, reward_amount
        )
        native_votes, _ = get_payable_votes(
            native_bribe,
            snapshot_date,
            reward_amount=reward_amount,
        )
        self.assertSetEqual(
            expected_native_ids, set(native_votes.values_list("id", flat=True))
        )

        issuer = Keypair.random().public_key
        non_native_market = self._make_market()
        non_native_bribe = self._make_bribe(
            non_native_market,
            asset_code="AQUA",
            asset_issuer=issuer,
            total=Decimal("0.0007000"),
        )

        valid_vote_1 = self._make_vote(
            non_native_market, Keypair.random().public_key, snapshot_date, "250000"
        )
        valid_vote_2 = self._make_vote(
            non_native_market, Keypair.random().public_key, snapshot_date, "250000"
        )
        late_trustline_vote = self._make_vote(
            non_native_market, Keypair.random().public_key, snapshot_date, "250000"
        )
        dust_vote = self._make_vote(
            non_native_market, Keypair.random().public_key, snapshot_date, "1"
        )
        delegated_vote = self._make_vote(
            non_native_market,
            Keypair.random().public_key,
            snapshot_date,
            "250000",
            has_delegation=True,
        )

        for vote in [valid_vote_1, valid_vote_2, dust_vote, delegated_vote]:
            self._make_holder(
                vote.voting_account, "AQUA", issuer, self._at(snapshot_date, 12)
            )
        self._make_holder(
            late_trustline_vote.voting_account,
            "AQUA",
            issuer,
            self._at(snapshot_date + timedelta(days=1), 1),
        )

        expected_non_native_ids = self._old_payable_vote_ids(
            non_native_bribe, snapshot_date, reward_amount
        )
        non_native_votes, _ = get_payable_votes(
            non_native_bribe,
            snapshot_date,
            reward_amount=reward_amount,
        )
        self.assertSetEqual(
            expected_non_native_ids, set(non_native_votes.values_list("id", flat=True))
        )

    def test_eligibility_non_native_requires_trustline(self):
        snapshot_date = timezone.now().date()
        market = self._make_market()
        issuer = Keypair.random().public_key
        bribe = self._make_bribe(
            market,
            asset_code="AQUA",
            asset_issuer=issuer,
        )
        valid_vote_1 = self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "100"
        )
        valid_vote_2 = self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "200"
        )
        outside_window_vote = self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "300"
        )

        self._make_holder(
            valid_vote_1.voting_account, "AQUA", issuer, self._at(snapshot_date, 1)
        )
        self._make_holder(
            valid_vote_2.voting_account, "AQUA", issuer, self._at(snapshot_date, 23)
        )
        self._make_holder(
            outside_window_vote.voting_account,
            "AQUA",
            issuer,
            self._at(snapshot_date + timedelta(days=1), 1),
        )

        returned_votes, total_votes = get_payable_votes(bribe, snapshot_date)

        self.assertSetEqual(
            set(returned_votes.values_list("id", flat=True)),
            {valid_vote_1.id, valid_vote_2.id},
        )
        self.assertEqual(
            total_votes, valid_vote_1.votes_value + valid_vote_2.votes_value
        )
        self.assertNotIn(
            outside_window_vote.id, returned_votes.values_list("id", flat=True)
        )

    def test_eligibility_dust_filter_with_reward_amount(self):
        snapshot_date = timezone.now().date()
        market = self._make_market()
        bribe = self._make_bribe(
            market,
            asset_code=Asset.native().code,
            asset_issuer="",
            total=Decimal("0.0007000"),
        )
        regular_votes, dust_vote = self._make_dust_votes(market, snapshot_date)

        returned_votes, _ = get_payable_votes(
            bribe,
            snapshot_date,
            reward_amount=bribe.daily_amount,
        )

        self.assertEqual(returned_votes.count(), len(regular_votes))
        self.assertNotIn(dust_vote.id, returned_votes.values_list("id", flat=True))

    def _make_rewardpayer_with_mocked_server(self, bribe, mock_server):
        wallet = SecuredWallet(public_key="G" + "A" * 55, secret="S" + "A" * 55)
        payer = RewardPayer(bribe, wallet, bribe.asset, Decimal("100"), stop_at=None)
        payer.server = mock_server
        return payer

    def test_process_page_build_failure_logs_sentry_and_persists_failed_payouts(self):
        import aquarius_bribes.rewards.reward_payer as rp_module

        snapshot_date = timezone.now().date()
        market = self._make_market()
        bribe = self._make_bribe(
            market, asset_code=Asset.native().code, asset_issuer=""
        )
        for _ in range(3):
            self._make_vote(
                market, Keypair.random().public_key, snapshot_date, "100000"
            )

        mock_server = mock.MagicMock()
        mock_server.load_account = mock.MagicMock(
            side_effect=RuntimeError("horizon boom")
        )
        payer = self._make_rewardpayer_with_mocked_server(bribe, mock_server)

        votes = VoteSnapshot.objects.filter(
            market_key=market, snapshot_time=snapshot_date
        )
        total_votes = votes.aggregate(total=models.Sum("votes_value"))["total"]

        with mock.patch.object(rp_module, "sentry_sdk") as mock_sentry:
            payer.pay_reward(votes, total_votes=total_votes)

        mock_sentry.capture_exception.assert_called()
        call_arg = mock_sentry.capture_exception.call_args[0][0]
        self.assertIsInstance(call_arg, RuntimeError)
        self.assertIn("horizon boom", str(call_arg))

        payouts = Payout.objects.filter(bribe=bribe)
        self.assertEqual(payouts.count(), 3)
        for payout in payouts:
            self.assertEqual(payout.status, Payout.STATUS_FAILED)
            self.assertEqual(payout.stellar_transaction_id, "")
            self.assertTrue(payout.message.startswith("build_failure: RuntimeError:"))
            self.assertIn("horizon boom", payout.message)

    def test_process_page_build_failure_allows_next_bribe_to_run(self):
        import aquarius_bribes.rewards.reward_payer as rp_module

        snapshot_date = timezone.now().date()

        # Bribe 1 — build will fail
        market1 = self._make_market()
        bribe1 = self._make_bribe(
            market1, asset_code=Asset.native().code, asset_issuer=""
        )
        self._make_vote(market1, Keypair.random().public_key, snapshot_date, "100000")

        # Bribe 2 — build will succeed
        market2 = self._make_market()
        bribe2 = self._make_bribe(
            market2, asset_code=Asset.native().code, asset_issuer=""
        )
        self._make_vote(market2, Keypair.random().public_key, snapshot_date, "200000")

        # Mock envelope for bribe2
        mock_envelope2 = mock.MagicMock()
        mock_envelope2.sign = mock.MagicMock()
        mock_envelope2.hash_hex = mock.MagicMock(return_value="hexhex2")

        # Payer for bribe2 — submit_transaction returns success
        mock_server2 = mock.MagicMock()
        mock_server2.submit_transaction = mock.MagicMock(
            return_value={"successful": True, "hash": "aabbcc"}
        )

        wallet = SecuredWallet(public_key="G" + "A" * 55, secret="S" + "A" * 55)
        payer1 = RewardPayer(bribe1, wallet, bribe1.asset, Decimal("100"), stop_at=None)
        payer2 = RewardPayer(bribe2, wallet, bribe2.asset, Decimal("100"), stop_at=None)
        payer2.server = mock_server2

        votes1 = VoteSnapshot.objects.filter(
            market_key=market1, snapshot_time=snapshot_date
        )
        total1 = votes1.aggregate(total=models.Sum("votes_value"))["total"]
        votes2 = VoteSnapshot.objects.filter(
            market_key=market2, snapshot_time=snapshot_date
        )
        total2 = votes2.aggregate(total=models.Sum("votes_value"))["total"]

        with mock.patch.object(rp_module, "sentry_sdk"):
            with mock.patch.object(
                payer1, "_build_transaction", side_effect=RuntimeError("bribe1 fails")
            ):
                payer1.pay_reward(votes1, total_votes=total1)

        with mock.patch.object(
            payer2, "_build_transaction", return_value=mock_envelope2
        ):
            payer2.pay_reward(votes2, total_votes=total2)

        bribe2_payouts = Payout.objects.filter(bribe=bribe2)
        self.assertEqual(bribe2_payouts.count(), 1)
        for payout in bribe2_payouts:
            self.assertEqual(payout.status, Payout.STATUS_SUCCESS)
            self.assertEqual(payout.stellar_transaction_id, "aabbcc")

    def test_process_page_unknown_response_treated_as_failure(self):
        snapshot_date = timezone.now().date()
        market = self._make_market()
        bribe = self._make_bribe(
            market, asset_code=Asset.native().code, asset_issuer=""
        )
        for _ in range(2):
            self._make_vote(
                market, Keypair.random().public_key, snapshot_date, "100000"
            )

        mock_envelope = mock.MagicMock()
        mock_envelope.sign = mock.MagicMock()
        mock_envelope.hash_hex = mock.MagicMock(return_value="hexhex")

        mock_server = mock.MagicMock()
        mock_server.submit_transaction = mock.MagicMock(
            return_value={"hash": "deadbeef"}
        )

        votes = VoteSnapshot.objects.filter(
            market_key=market, snapshot_time=snapshot_date
        )
        total_votes = votes.aggregate(total=models.Sum("votes_value"))["total"]

        wallet = SecuredWallet(public_key="G" + "A" * 55, secret="S" + "A" * 55)
        payer = RewardPayer(bribe, wallet, bribe.asset, Decimal("100"), stop_at=None)
        payer.server = mock_server

        with mock.patch.object(payer, "_build_transaction", return_value=mock_envelope):
            payer.pay_reward(votes, total_votes=total_votes)

        payouts = Payout.objects.filter(bribe=bribe)
        self.assertEqual(payouts.count(), 2)
        for payout in payouts:
            self.assertEqual(payout.status, Payout.STATUS_FAILED)
            self.assertEqual(payout.message, "unknown_response_no_successful_field")

    def test_cache_flag_ttl_applied(self):
        from aquarius_bribes.rewards.tasks import (
            LOAD_TRUSTORS_TASK_ACTIVE_KEY,
            LOAD_TRUSTORS_TASK_TTL,
            LOAD_VOTES_TASK_ACTIVE_KEY,
            LOAD_VOTES_TASK_TTL,
        )

        with mock.patch("aquarius_bribes.rewards.tasks.cache") as mock_cache:
            with mock.patch("aquarius_bribes.rewards.tasks.task_make_claims_snapshot"):
                with mock.patch("aquarius_bribes.rewards.tasks.VotesLoader"):
                    with mock.patch(
                        "aquarius_bribes.rewards.tasks.AggregatedByAssetBribe.objects"
                    ) as mock_objects:
                        mock_objects.filter.return_value.values_list.return_value.distinct.return_value = []
                        task_load_votes()

        mock_cache.set.assert_any_call(
            LOAD_VOTES_TASK_ACTIVE_KEY, True, LOAD_VOTES_TASK_TTL
        )

        with mock.patch("aquarius_bribes.rewards.tasks.cache") as mock_cache:
            with mock.patch("aquarius_bribes.rewards.tasks.TrusteesLoader"):
                with mock.patch(
                    "aquarius_bribes.rewards.tasks.AggregatedByAssetBribe.objects"
                ) as mock_objects:
                    mock_objects.filter.return_value = []
                    task_make_trustees_snapshot()

        mock_cache.set.assert_any_call(
            LOAD_TRUSTORS_TASK_ACTIVE_KEY, True, LOAD_TRUSTORS_TASK_TTL
        )

    def test_pay_rewards_skips_when_fix_db_flag_set(self):
        from django.core.cache import cache
        from aquarius_bribes.rewards.tasks import (
            PAY_REWARDS_FIX_DB_ACTIVE_KEY,
            PAY_REWARDS_TASK_ACTIVE_KEY,
        )

        cache.set(PAY_REWARDS_FIX_DB_ACTIVE_KEY, True, 60)

        with mock.patch(
            "aquarius_bribes.rewards.tasks.AggregatedByAssetBribe.objects.filter"
        ) as mock_filter:
            task_pay_rewards()

        self.assertFalse(mock_filter.called)
        self.assertFalse(cache.get(PAY_REWARDS_TASK_ACTIVE_KEY, False))
        cache.delete(PAY_REWARDS_FIX_DB_ACTIVE_KEY)

    def test_pay_rewards_sets_and_clears_active_flag(self):
        from django.core.cache import cache
        from aquarius_bribes.rewards.tasks import PAY_REWARDS_TASK_ACTIVE_KEY

        def assert_flag_set(*args, **kwargs):
            self.assertTrue(cache.get(PAY_REWARDS_TASK_ACTIVE_KEY, False))
            return []

        with mock.patch("aquarius_bribes.rewards.tasks.SecuredWallet"):
            with mock.patch(
                "aquarius_bribes.rewards.tasks.AggregatedByAssetBribe.objects.filter",
                side_effect=assert_flag_set,
            ):
                task_pay_rewards()

        self.assertFalse(cache.get(PAY_REWARDS_TASK_ACTIVE_KEY, False))

        with mock.patch("aquarius_bribes.rewards.tasks.SecuredWallet"):
            with mock.patch(
                "aquarius_bribes.rewards.tasks.AggregatedByAssetBribe.objects.filter",
                side_effect=RuntimeError("boom"),
            ):
                with self.assertRaises(RuntimeError):
                    task_pay_rewards()

        self.assertFalse(cache.get(PAY_REWARDS_TASK_ACTIVE_KEY, False))

    def test_pay_rewards_refuses_when_own_key_already_set(self):
        # Self-exclusion: if another task_pay_rewards run already owns
        # PAY_REWARDS_TASK_ACTIVE_KEY, a second invocation must return
        # without computing the payable set — otherwise two workers
        # double-submit payouts to the same voter.
        from django.core.cache import cache
        from aquarius_bribes.rewards.tasks import PAY_REWARDS_TASK_ACTIVE_KEY

        cache.set(PAY_REWARDS_TASK_ACTIVE_KEY, "other-owner", 60)
        try:
            with mock.patch(
                "aquarius_bribes.rewards.tasks.AggregatedByAssetBribe.objects.filter"
            ) as mock_filter:
                task_pay_rewards()

            self.assertFalse(mock_filter.called)
            # The other owner's token must remain — a refused run never
            # clears a key it does not own.
            self.assertEqual(
                cache.get(PAY_REWARDS_TASK_ACTIVE_KEY), "other-owner"
            )
        finally:
            cache.delete(PAY_REWARDS_TASK_ACTIVE_KEY)

    def test_reconcile_buckets_matched(self):
        snapshot_date = timezone.now().date()
        created_at = self._at(snapshot_date, 12)
        wallet = Keypair.random().public_key
        market = self._make_market(
            asset1="AQUA:{}".format(Keypair.random().public_key),
            asset2="native",
        )
        bribe = self._make_day_bribe(
            market,
            snapshot_date,
            asset_code=Asset.native().code,
            asset_issuer="",
        )
        vote_a = self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "100"
        )
        vote_b = self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "200"
        )
        records = [
            self._chain_record(
                wallet,
                vote_a.voting_account,
                Asset.native().code,
                "",
                Decimal("10.0000000"),
                "tx-a",
                "Bribe: {}".format(market.short_value),
                created_at.isoformat(),
            ),
            self._chain_record(
                wallet,
                vote_b.voting_account,
                Asset.native().code,
                "",
                Decimal("20.0000000"),
                "tx-b",
                "Bribe: {}".format(market.short_value),
                created_at.isoformat(),
            ),
        ]
        server, _ = self._mock_reconcile_server(records, [])
        self._make_payout(bribe, vote_a, "tx-a", Decimal("10.0000000"))
        self._make_payout(bribe, vote_b, "tx-b", Decimal("20.0000000"))

        report = self._reconcile(snapshot_date, snapshot_date, wallet, server=server)

        self.assertEqual(len(report.matched), 2)
        self.assertEqual(len(report.chain_only), 0)
        self.assertEqual(len(report.db_only), 0)
        self.assertEqual(len(report.ambiguous), 0)
        self.assertEqual(len(report.missed), 0)

    def test_reconcile_buckets_chain_only(self):
        snapshot_date = timezone.now().date()
        created_at = self._at(snapshot_date, 12)
        wallet = Keypair.random().public_key
        market = self._make_market(
            asset1="AQUA:{}".format(Keypair.random().public_key),
            asset2="native",
        )
        bribe = self._make_day_bribe(
            market,
            snapshot_date,
            asset_code=Asset.native().code,
            asset_issuer="",
        )
        vote_a = self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "100"
        )
        vote_b = self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "200"
        )
        server, _ = self._mock_reconcile_server(
            [
                self._chain_record(
                    wallet,
                    vote_a.voting_account,
                    Asset.native().code,
                    "",
                    Decimal("10.0000000"),
                    "tx-a",
                    "Bribe: {}".format(market.short_value),
                    created_at.isoformat(),
                ),
                self._chain_record(
                    wallet,
                    vote_b.voting_account,
                    Asset.native().code,
                    "",
                    Decimal("20.0000000"),
                    "tx-b",
                    "Bribe: {}".format(market.short_value),
                    created_at.isoformat(),
                ),
            ],
            [],
        )
        failed_payout = self._make_payout(
            bribe,
            vote_b,
            "tx-b",
            Decimal("20.0000000"),
            status=Payout.STATUS_FAILED,
        )

        report = self._reconcile(snapshot_date, snapshot_date, wallet, server=server)

        self.assertEqual(len(report.matched), 0)
        self.assertEqual(len(report.chain_only), 2)
        self.assertEqual(len(report.db_only), 0)
        self.assertEqual(len(report.missed), 0)
        chain_only_by_account = {entry[1]: entry for entry in report.chain_only}
        self.assertIsNone(chain_only_by_account[vote_a.voting_account][9])
        self.assertEqual(
            chain_only_by_account[vote_b.voting_account][9], failed_payout.id
        )

    def test_reconcile_buckets_db_only(self):
        snapshot_date = timezone.now().date()
        wallet = Keypair.random().public_key
        market = self._make_market()
        bribe = self._make_bribe(
            market,
            asset_code=Asset.native().code,
            asset_issuer="",
        )
        vote = self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "100"
        )
        payout = self._make_payout(bribe, vote, "abc123", Decimal("15.0000000"))
        server, _ = self._mock_reconcile_server([], [])

        report = self._reconcile(snapshot_date, snapshot_date, wallet, server=server)

        self.assertEqual(len(report.db_only), 1)
        self.assertEqual(report.db_only[0][0], payout.id)
        self.assertEqual(len(report.matched), 0)
        self.assertEqual(len(report.chain_only), 0)
        self.assertEqual(len(report.missed), 0)

    def test_reconcile_buckets_missed(self):
        snapshot_date = timezone.now().date()
        wallet = Keypair.random().public_key
        market = self._make_market()
        self._make_bribe(
            market,
            asset_code=Asset.native().code,
            asset_issuer="",
        )
        votes = [
            self._make_vote(market, Keypair.random().public_key, snapshot_date, "100"),
            self._make_vote(market, Keypair.random().public_key, snapshot_date, "200"),
            self._make_vote(market, Keypair.random().public_key, snapshot_date, "300"),
        ]
        server, _ = self._mock_reconcile_server([], [])

        report = self._reconcile(snapshot_date, snapshot_date, wallet, server=server)

        self.assertEqual(len(report.missed), 3)
        self.assertSetEqual(
            {item[1] for item in report.missed}, {vote.id for vote in votes}
        )

    def test_reconcile_buckets_ambiguous(self):
        snapshot_date = timezone.now().date()
        created_at = self._at(snapshot_date, 12)
        wallet = Keypair.random().public_key
        market = self._make_market(
            asset1="AQUA:{}".format(Keypair.random().public_key),
            asset2="native",
        )
        self._make_day_bribe(
            market,
            snapshot_date,
            asset_code=Asset.native().code,
            asset_issuer="",
        )
        self._make_day_bribe(
            market,
            snapshot_date,
            asset_code=Asset.native().code,
            asset_issuer="",
        )
        server, _ = self._mock_reconcile_server(
            [
                self._chain_record(
                    wallet,
                    Keypair.random().public_key,
                    Asset.native().code,
                    "",
                    Decimal("10.0000000"),
                    "tx-a",
                    "Bribe: {}".format(market.short_value),
                    created_at.isoformat(),
                ),
            ],
            [],
        )

        report = self._reconcile(snapshot_date, snapshot_date, wallet, server=server)

        # Destination has no VoteSnapshot → _resolve_chain_only_candidate
        # returns _CHAIN_ORPHAN. The fallback bribe lookup finds 2 same-day
        # bribes for the memo short_value, so fallback_bribe_id is None and
        # the op lands in chain_only (not ambiguous). Ambiguous is reserved
        # for situations where a VoteSnapshot exists but cannot be uniquely
        # tied to a (bribe, vote_snapshot) — covered by dedicated tests.
        self.assertEqual(len(report.chain_only), 1)
        self.assertIsNone(report.chain_only[0][5])  # fallback_bribe_id
        self.assertEqual(len(report.ambiguous), 0)
        self.assertEqual(len(report.matched), 0)
        self.assertEqual(len(report.missed), 0)

    def test_reconcile_missed_ignores_dust_votes(self):
        snapshot_date = timezone.now().date()
        wallet = Keypair.random().public_key
        market = self._make_market()
        self._make_bribe(
            market,
            asset_code=Asset.native().code,
            asset_issuer="",
            total=Decimal("0.0007000"),
        )
        regular_votes, dust_vote = self._make_dust_votes(market, snapshot_date)
        server, _ = self._mock_reconcile_server([], [])

        report = self._reconcile(snapshot_date, snapshot_date, wallet, server=server)

        self.assertEqual(len(report.missed), 4)
        self.assertSetEqual(
            {item[1] for item in report.missed}, {vote.id for vote in regular_votes}
        )
        self.assertNotIn(dust_vote.id, {item[1] for item in report.missed})

    def test_reconcile_resolution_aqua_usdc_vs_aqua_usdt_disambiguated_by_asset_issuer(
        self,
    ):
        snapshot_date = timezone.now().date()
        created_at = self._at(snapshot_date, 12)
        wallet = Keypair.random().public_key
        usdc_issuer = Keypair.random().public_key
        usdt_issuer = Keypair.random().public_key
        market_usdc = self._make_market(
            asset1="AQUA:{}".format(Keypair.random().public_key),
            asset2="USD:{}".format(usdc_issuer),
        )
        market_usdt = self._make_market(
            asset1="AQUA:{}".format(Keypair.random().public_key),
            asset2="USD:{}".format(usdt_issuer),
        )
        bribe_usdc = self._make_day_bribe(
            market_usdc,
            snapshot_date,
            asset_code="USD",
            asset_issuer=usdc_issuer,
        )
        self._make_day_bribe(
            market_usdt,
            snapshot_date,
            asset_code="USD",
            asset_issuer=usdt_issuer,
        )
        vote = self._make_vote(
            market_usdc, Keypair.random().public_key, snapshot_date, "100"
        )
        server, _ = self._mock_reconcile_server(
            [
                self._chain_record(
                    wallet,
                    vote.voting_account,
                    "USD",
                    usdc_issuer,
                    Decimal("10.0000000"),
                    "tx-a",
                    "Bribe: AQUA/USD",
                    created_at.isoformat(),
                ),
            ],
            [],
        )

        report = self._reconcile(snapshot_date, snapshot_date, wallet, server=server)

        self.assertEqual(len(report.chain_only), 1)
        self.assertEqual(report.chain_only[0][5], bribe_usdc.id)
        self.assertEqual(len(report.ambiguous), 0)
        self.assertEqual(len(report.missed), 0)

    def test_reconcile_resolution_orphan_chain_payment_no_vote_snapshot(self):
        snapshot_date = timezone.now().date()
        created_at = self._at(snapshot_date, 12)
        wallet = Keypair.random().public_key
        market = self._make_market(
            asset1="AQUA:{}".format(Keypair.random().public_key),
            asset2="native",
        )
        self._make_day_bribe(
            market,
            snapshot_date,
            asset_code=Asset.native().code,
            asset_issuer="",
        )
        server, _ = self._mock_reconcile_server(
            [
                self._chain_record(
                    wallet,
                    Keypair.random().public_key,
                    Asset.native().code,
                    "",
                    Decimal("10.0000000"),
                    "tx-a",
                    "Bribe: {}".format(market.short_value),
                    created_at.isoformat(),
                ),
            ],
            [],
        )

        report = self._reconcile(snapshot_date, snapshot_date, wallet, server=server)

        self.assertEqual(len(report.chain_only), 1)
        self.assertIsNone(report.chain_only[0][6])
        self.assertEqual(report.chain_orphan_count, 1)

    def test_reconcile_partial_loss_inside_successful_multi_op_tx(self):
        snapshot_date = timezone.now().date()
        created_at = self._at(snapshot_date, 12)
        wallet = Keypair.random().public_key
        market = self._make_market(
            asset1="AQUA:{}".format(Keypair.random().public_key),
            asset2="native",
        )
        bribe = self._make_day_bribe(
            market,
            snapshot_date,
            asset_code=Asset.native().code,
            asset_issuer="",
        )
        votes = [
            self._make_vote(market, Keypair.random().public_key, snapshot_date, "100"),
            self._make_vote(market, Keypair.random().public_key, snapshot_date, "200"),
            self._make_vote(market, Keypair.random().public_key, snapshot_date, "300"),
            self._make_vote(market, Keypair.random().public_key, snapshot_date, "400"),
            self._make_vote(market, Keypair.random().public_key, snapshot_date, "500"),
        ]
        records = [
            self._chain_record(
                wallet,
                vote.voting_account,
                Asset.native().code,
                "",
                Decimal("{:.7f}".format(index + 1)),
                "shared-tx",
                "Bribe: {}".format(market.short_value),
                created_at.isoformat(),
                paging_token="shared-tx-{}".format(index),
            )
            for index, vote in enumerate(votes)
        ]
        server, _ = self._mock_reconcile_server(records, [])
        for index, vote in enumerate(votes[:3]):
            self._make_payout(bribe, vote, "shared-tx", Decimal("{:.7f}".format(index + 1)))

        report = self._reconcile(snapshot_date, snapshot_date, wallet, server=server)

        self.assertEqual(len(report.matched), 3)
        self.assertEqual(len(report.chain_only), 2)
        self.assertSetEqual({entry[0] for entry in report.matched}, {"shared-tx"})
        self.assertSetEqual({entry[0] for entry in report.chain_only}, {"shared-tx"})

    def test_reconcile_matches_midnight_crossing_payout(self):
        # task_pay_rewards for snapshot_date=D submitted tx after midnight,
        # so the chain op carries created_at=D+1 even though the Payout is
        # linked to snapshot_time=D. Reconcile must still classify as
        # matched; without the ±1 day chain window + Payout-first lookup,
        # the Payout would be flagged db_only and --fix-db would downgrade
        # a valid success to FAILED.
        snapshot_date = timezone.now().date() - timedelta(days=2)
        next_day = snapshot_date + timedelta(days=1)
        created_at = self._at(next_day, 0) + timedelta(minutes=2)
        wallet = Keypair.random().public_key
        market = self._make_market(
            asset1="AQUA:{}".format(Keypair.random().public_key),
            asset2="native",
        )
        bribe = self._make_bribe(
            market,
            asset_code=Asset.native().code,
            asset_issuer="",
            start=self._at(snapshot_date, 0) - timedelta(days=1),
            stop=self._at(snapshot_date, 0) + timedelta(days=7),
        )
        vote = self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "100"
        )
        payout = self._make_payout(bribe, vote, "tx-midnight", Decimal("10.0000000"))
        records = [
            self._chain_record(
                wallet,
                vote.voting_account,
                Asset.native().code,
                "",
                Decimal("10.0000000"),
                "tx-midnight",
                "Bribe: {}".format(market.short_value),
                created_at.isoformat(),
            ),
        ]
        server, _ = self._mock_reconcile_server(records, [])

        report = self._reconcile(
            snapshot_date, snapshot_date, wallet, server=server
        )

        self.assertEqual(len(report.matched), 1)
        self.assertEqual(report.matched[0][0], "tx-midnight")
        self.assertEqual(report.matched[0][7], payout.id)
        self.assertEqual(len(report.db_only), 0)
        self.assertEqual(len(report.missed), 0)

    def test_reconcile_matches_midnight_cross_on_bribes_last_day(self):
        # Bribe's active period ends at D+1 00:00 UTC (inclusive of day D,
        # exclusive of D+1). task_pay_rewards for snapshot_date=D runs late
        # and submits tx at D+1 00:05 UTC. The chain op's created_at is
        # past the bribe's stop_at, so candidate resolution by
        # (start_at__lte=created_at, stop_at__gt=created_at) returned
        # zero and the op was dropped into skipped_no_active_bribe. A
        # legitimate chain-to-DB match could never be reconciled on the
        # final day of a bribe. Payout-first lookup — and widened bribe
        # window — fix that.
        snapshot_date = timezone.now().date() - timedelta(days=2)
        next_day = snapshot_date + timedelta(days=1)
        created_at = self._at(next_day, 0) + timedelta(minutes=5)
        wallet = Keypair.random().public_key
        market = self._make_market(
            asset1="AQUA:{}".format(Keypair.random().public_key),
            asset2="native",
        )
        bribe = self._make_bribe(
            market,
            asset_code=Asset.native().code,
            asset_issuer="",
            start=self._at(snapshot_date, 0) - timedelta(days=6),
            stop=self._at(next_day, 0),
        )
        vote = self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "100"
        )
        payout = self._make_payout(bribe, vote, "tx-last-day", Decimal("10.0000000"))
        records = [
            self._chain_record(
                wallet,
                vote.voting_account,
                Asset.native().code,
                "",
                Decimal("10.0000000"),
                "tx-last-day",
                "Bribe: {}".format(market.short_value),
                created_at.isoformat(),
            ),
        ]
        server, _ = self._mock_reconcile_server(records, [])

        report = self._reconcile(
            snapshot_date, snapshot_date, wallet, server=server
        )

        self.assertEqual(len(report.matched), 1)
        self.assertEqual(report.matched[0][0], "tx-last-day")
        self.assertEqual(report.matched[0][7], payout.id)
        self.assertEqual(report.skipped_no_active_bribe, 0)
        self.assertEqual(len(report.db_only), 0)
        self.assertEqual(len(report.missed), 0)

    def test_reconcile_disambiguates_delegatee_via_payout(self):
        # A delegatee has multiple VoteSnapshots for one
        # (market_key, voting_account, snapshot_time) with has_delegation=False
        # — one for their own direct vote (is_delegated=False) and one per
        # incoming delegation (is_delegated=True, delegate_owner=<delegator>).
        # Legacy lookup-by-VoteSnapshot flagged such tx as ambiguous and
        # --fix-db refused to touch them. Payout-first lookup resolves the
        # VoteSnapshot via the 5-tuple (tx_hash, destination, asset, amount,
        # reward_amount) which maps 1:1 to a specific VoteSnapshot.
        snapshot_date = timezone.now().date()
        created_at = self._at(snapshot_date, 12)
        wallet = Keypair.random().public_key
        market = self._make_market(
            asset1="AQUA:{}".format(Keypair.random().public_key),
            asset2="native",
        )
        bribe = self._make_day_bribe(
            market,
            snapshot_date,
            asset_code=Asset.native().code,
            asset_issuer="",
        )
        delegatee = Keypair.random().public_key
        delegator = Keypair.random().public_key
        direct_vote = VoteSnapshot.objects.create(
            market_key=market,
            voting_account=delegatee,
            votes_value=Decimal("100"),
            snapshot_time=snapshot_date,
            has_delegation=False,
            is_delegated=False,
            delegate_owner=None,
        )
        delegated_vote = VoteSnapshot.objects.create(
            market_key=market,
            voting_account=delegatee,
            votes_value=Decimal("250"),
            snapshot_time=snapshot_date,
            has_delegation=False,
            is_delegated=True,
            delegate_owner=delegator,
        )
        direct_payout = self._make_payout(bribe, direct_vote, "tx-direct", Decimal("5.0000000"))
        delegated_payout = self._make_payout(
            bribe,
            delegated_vote,
            "tx-delegated",
            Decimal("12.5000000"),
        )
        records = [
            self._chain_record(
                wallet,
                delegatee,
                Asset.native().code,
                "",
                Decimal("5.0000000"),
                "tx-direct",
                "Bribe: {}".format(market.short_value),
                created_at.isoformat(),
            ),
            self._chain_record(
                wallet,
                delegatee,
                Asset.native().code,
                "",
                Decimal("12.5000000"),
                "tx-delegated",
                "Bribe: {}".format(market.short_value),
                created_at.isoformat(),
                paging_token="tx-delegated-paging",
            ),
        ]
        server, _ = self._mock_reconcile_server(records, [])

        report = self._reconcile(
            snapshot_date, snapshot_date, wallet, server=server
        )

        self.assertEqual(len(report.matched), 2)
        self.assertEqual(len(report.ambiguous), 0)
        matched_by_tx = {entry[0]: entry for entry in report.matched}
        self.assertEqual(matched_by_tx["tx-direct"][6], direct_vote.id)
        self.assertEqual(matched_by_tx["tx-direct"][7], direct_payout.id)
        self.assertEqual(matched_by_tx["tx-delegated"][6], delegated_vote.id)
        self.assertEqual(matched_by_tx["tx-delegated"][7], delegated_payout.id)

    def test_reconcile_does_not_mutate_rows_outside_requested_window(self):
        # X7: --fix-db --from=D --to=D must never create, upgrade, or
        # downgrade Payouts whose vote_snapshot.snapshot_time lies outside
        # [D, D]. The chain walk still pads by ±1 day for Counter dedup
        # and midnight-crossing matches, but Payout / VoteSnapshot lookups
        # must intersect with the operator's explicit window.
        snapshot_date = timezone.now().date() - timedelta(days=5)
        prev_date = snapshot_date - timedelta(days=1)
        wallet = Keypair.random().public_key
        market = self._make_market(
            asset1="AQUA:{}".format(Keypair.random().public_key),
            asset2="native",
        )
        bribe = self._make_bribe(
            market,
            asset_code=Asset.native().code,
            asset_issuer="",
            start=self._at(prev_date, 0) - timedelta(days=1),
            stop=self._at(snapshot_date, 0) + timedelta(days=2),
        )
        # Out-of-window: vote and failed Payout on prev_date. A chain
        # payment that matches this row exists too. Without the fix,
        # --fix-db --from=snapshot_date --to=snapshot_date would upgrade
        # the failed Payout for prev_date silently.
        out_of_window_vote = self._make_vote(
            market, Keypair.random().public_key, prev_date, "100"
        )
        out_of_window_failed_payout = self._make_payout(
            bribe,
            out_of_window_vote,
            "tx-prev-day",
            Decimal("7.0000000"),
            status=Payout.STATUS_FAILED,
        )
        # Out-of-window: pure-chain payment on prev_date with no Payout
        # but with a matching VoteSnapshot. Without the fix, the
        # VoteSnapshot fallback would attach this op to a chain_only
        # entry and apply_fix_db would create a Payout for prev_date.
        pure_chain_voter = Keypair.random().public_key
        self._make_vote(market, pure_chain_voter, prev_date, "200")
        records = [
            self._chain_record(
                wallet,
                out_of_window_vote.voting_account,
                Asset.native().code,
                "",
                Decimal("7.0000000"),
                "tx-prev-day",
                "Bribe: {}".format(market.short_value),
                self._at(prev_date, 12).isoformat(),
            ),
            self._chain_record(
                wallet,
                pure_chain_voter,
                Asset.native().code,
                "",
                Decimal("13.0000000"),
                "tx-prev-day-pure",
                "Bribe: {}".format(market.short_value),
                self._at(prev_date, 12).isoformat(),
                paging_token="tx-prev-day-pure",
            ),
        ]
        server, _ = self._mock_reconcile_server(records, [])

        report = self._reconcile(snapshot_date, snapshot_date, wallet, server=server)

        # Neither out-of-window op lands in any mutating bucket.
        chain_only_tx_hashes = {row[0] for row in report.chain_only}
        self.assertNotIn("tx-prev-day", chain_only_tx_hashes)
        self.assertNotIn("tx-prev-day-pure", chain_only_tx_hashes)
        matched_tx_hashes = {row[0] for row in report.matched}
        self.assertNotIn("tx-prev-day", matched_tx_hashes)

        # apply_fix_db must not mutate either row.
        from aquarius_bribes.rewards.reconcile import apply_fix_db

        payout_count_before = Payout.objects.count()
        counts = apply_fix_db(report, now=timezone.now())
        self.assertEqual(counts["chain_only_created"], 0)
        self.assertEqual(counts["chain_only_upgraded"], 0)
        self.assertEqual(counts["db_only_downgraded"], 0)
        self.assertEqual(Payout.objects.count(), payout_count_before)
        out_of_window_failed_payout.refresh_from_db()
        self.assertEqual(out_of_window_failed_payout.status, Payout.STATUS_FAILED)

    def test_reconcile_does_not_mutate_rows_past_to_date(self):
        # X6: mirror of X7's prior-day guard for the symmetric future-day
        # side. The chain pad `chain_to = to_date + 1` brings today's chain
        # ops into the loop when the operator runs `--fix-db --to D-1` on
        # day D. Without the X6 guard the resolver's single-VS branch would
        # silently return `(bribe, VS_{D-1})` for an op whose real Payout
        # exists on `VS_D` — `apply_fix_db.get_or_create` would then
        # fabricate a SUCCESS Payout misattributing today's tx_hash to
        # yesterday's snapshot.
        today = timezone.now().date()
        d_chain = today  # chain op lands today (chain_date_for_payout > to_date)
        d_to = today - timedelta(days=1)
        d_from = today - timedelta(days=2)
        wallet = Keypair.random().public_key
        market = self._make_market(
            asset1="AQUA:{}".format(Keypair.random().public_key),
            asset2="native",
        )
        # Multi-day bribe spanning all three days so a memo+asset+short_value
        # match exists on both `d_to` (in window) and `d_chain` (past `to`).
        bribe = self._make_bribe(
            market,
            asset_code=Asset.native().code,
            asset_issuer="",
            start=self._at(d_from, 0) - timedelta(days=1),
            stop=self._at(d_chain, 0) + timedelta(days=2),
        )
        voter = Keypair.random().public_key
        # Voter has VS on both d_to (in window) and today.
        vote_to = self._make_vote(market, voter, d_to, "100")
        vote_today = self._make_vote(market, voter, d_chain, "100")
        # Today's task_pay_rewards ran and wrote a real Payout for VS_today.
        real_payout = self._make_payout(
            bribe,
            vote_today,
            "tx-today",
            Decimal("13.0000000"),
        )
        # The chain op for today's submission. Payout-first lookup will
        # MISS this Payout because the candidate `in_window_snapshot_times`
        # collapses to `[d_to]` (today is outside `[d_from, d_to]`),
        # excluding `vote_today`'s `snapshot_time`.
        records = [
            self._chain_record(
                wallet,
                voter,
                Asset.native().code,
                "",
                Decimal("13.0000000"),
                "tx-today",
                "Bribe: {}".format(market.short_value),
                self._at(d_chain, 10).isoformat(),
            ),
        ]
        server, _ = self._mock_reconcile_server(records, [])

        report = self._reconcile(d_from, d_to, wallet, server=server)

        # The future-day chain op must not enter any mutating bucket. It is
        # accounted for via the dedicated counter so the operator sees that
        # today's chain activity was acknowledged.
        self.assertEqual(report.skipped_future_window_chain_op, 1)
        chain_only_tx_hashes = {row[0] for row in report.chain_only}
        self.assertNotIn("tx-today", chain_only_tx_hashes)
        ambiguous_tx_hashes = {row[0] for row in report.ambiguous}
        self.assertNotIn("tx-today", ambiguous_tx_hashes)
        # Real Payout on VS_today is past `to_date` and so isn't in the
        # report's matched set either — but it must still exist intact.
        matched_tx_hashes = {row[0] for row in report.matched}
        self.assertNotIn("tx-today", matched_tx_hashes)

        # apply_fix_db must not fabricate a spurious Payout.
        from aquarius_bribes.rewards.reconcile import apply_fix_db

        payout_count_before = Payout.objects.count()
        counts = apply_fix_db(report, now=timezone.now())
        self.assertEqual(counts["chain_only_created"], 0)
        self.assertEqual(counts["chain_only_upgraded"], 0)
        self.assertEqual(counts["db_only_downgraded"], 0)
        self.assertEqual(Payout.objects.count(), payout_count_before)
        real_payout.refresh_from_db()
        self.assertEqual(real_payout.vote_snapshot_id, vote_today.id)
        self.assertEqual(real_payout.status, Payout.STATUS_SUCCESS)

    def test_reconcile_collision_5tuple_routed_to_ambiguous(self):
        # X4: two Payouts with identical (tx_hash, voting_account,
        # snapshot_time, asset, reward_amount) — e.g. one account that
        # delegated the same balance to two different delegatees in the
        # same market, paid in the same Horizon tx. The Payout-first
        # lookup cannot disambiguate them, so reconcile must route to
        # AMBIGUOUS and --fix-db must refuse to downgrade either row.
        snapshot_date = timezone.now().date()
        created_at = self._at(snapshot_date, 12)
        wallet = Keypair.random().public_key
        market = self._make_market(
            asset1="AQUA:{}".format(Keypair.random().public_key),
            asset2="native",
        )
        bribe = self._make_day_bribe(
            market,
            snapshot_date,
            asset_code=Asset.native().code,
            asset_issuer="",
        )
        shared_account = Keypair.random().public_key
        delegatee_1 = Keypair.random().public_key
        delegatee_2 = Keypair.random().public_key
        vote_to_d1 = VoteSnapshot.objects.create(
            market_key=market,
            voting_account=shared_account,
            votes_value=Decimal("100"),
            snapshot_time=snapshot_date,
            has_delegation=False,
            is_delegated=True,
            delegate_owner=delegatee_1,
        )
        vote_to_d2 = VoteSnapshot.objects.create(
            market_key=market,
            voting_account=shared_account,
            votes_value=Decimal("100"),
            snapshot_time=snapshot_date,
            has_delegation=False,
            is_delegated=True,
            delegate_owner=delegatee_2,
        )
        payout_1 = self._make_payout(bribe, vote_to_d1, "tx-batch", Decimal("5.0000000"))
        payout_2 = self._make_payout(bribe, vote_to_d2, "tx-batch", Decimal("5.0000000"))
        records = [
            self._chain_record(
                wallet,
                shared_account,
                Asset.native().code,
                "",
                Decimal("5.0000000"),
                "tx-batch",
                "Bribe: {}".format(market.short_value),
                created_at.isoformat(),
            ),
            self._chain_record(
                wallet,
                shared_account,
                Asset.native().code,
                "",
                Decimal("5.0000000"),
                "tx-batch",
                "Bribe: {}".format(market.short_value),
                created_at.isoformat(),
                paging_token="tx-batch-2",
            ),
        ]
        server, _ = self._mock_reconcile_server(records, [])

        report = self._reconcile(snapshot_date, snapshot_date, wallet, server=server)

        self.assertEqual(len(report.matched), 0)
        self.assertEqual(len(report.db_only), 0)
        # Both chain ops + both db-side Payouts surface as AMBIGUOUS.
        # The operator must resolve manually — --fix-db never touches
        # ambiguous rows.
        self.assertGreaterEqual(len(report.ambiguous), 2)

        # apply_fix_db must not touch either Payout.
        from aquarius_bribes.rewards.reconcile import apply_fix_db

        counts = apply_fix_db(report, now=timezone.now())
        self.assertEqual(counts["chain_only_created"], 0)
        self.assertEqual(counts["db_only_downgraded"], 0)
        payout_1.refresh_from_db()
        payout_2.refresh_from_db()
        self.assertEqual(payout_1.status, Payout.STATUS_SUCCESS)
        self.assertEqual(payout_2.status, Payout.STATUS_SUCCESS)

    def test_reconcile_missed_detects_partial_delegatee_payout(self):
        # X3: a single voting_account can hold multiple has_delegation=False
        # VoteSnapshots on the same date (own direct vote + incoming
        # delegations). If ONE of them was paid on-chain + has a Payout and
        # ANOTHER was never paid, the MISSED bucket must still flag the
        # unpaid snapshot. A per-account matched-set would swallow it.
        snapshot_date = timezone.now().date()
        created_at = self._at(snapshot_date, 12)
        wallet = Keypair.random().public_key
        market = self._make_market(
            asset1="AQUA:{}".format(Keypair.random().public_key),
            asset2="native",
        )
        bribe = self._make_day_bribe(
            market,
            snapshot_date,
            asset_code=Asset.native().code,
            asset_issuer="",
        )
        delegatee = Keypair.random().public_key
        delegator = Keypair.random().public_key
        direct_vote = VoteSnapshot.objects.create(
            market_key=market,
            voting_account=delegatee,
            votes_value=Decimal("100"),
            snapshot_time=snapshot_date,
            has_delegation=False,
            is_delegated=False,
            delegate_owner=None,
        )
        delegated_vote = VoteSnapshot.objects.create(
            market_key=market,
            voting_account=delegatee,
            votes_value=Decimal("250"),
            snapshot_time=snapshot_date,
            has_delegation=False,
            is_delegated=True,
            delegate_owner=delegator,
        )
        # Only the direct vote was paid — Payout + chain op both present.
        self._make_payout(bribe, direct_vote, "tx-direct", Decimal("5.0000000"))
        records = [
            self._chain_record(
                wallet,
                delegatee,
                Asset.native().code,
                "",
                Decimal("5.0000000"),
                "tx-direct",
                "Bribe: {}".format(market.short_value),
                created_at.isoformat(),
            ),
        ]
        server, _ = self._mock_reconcile_server(records, [])

        report = self._reconcile(
            snapshot_date, snapshot_date, wallet, server=server
        )

        self.assertEqual(len(report.matched), 1)
        self.assertEqual(len(report.chain_only), 0)
        self.assertEqual(len(report.db_only), 0)
        # delegated_vote has neither a Payout nor a chain op — must appear
        # in MISSED despite sharing voting_account with the matched row.
        missed_vote_ids = {entry[1] for entry in report.missed}
        self.assertIn(delegated_vote.id, missed_vote_ids)
        self.assertNotIn(direct_vote.id, missed_vote_ids)

    def test_reconcile_paginates_across_multiple_pages(self):
        snapshot_date = timezone.now().date()
        created_at = self._at(snapshot_date, 12)
        wallet = Keypair.random().public_key
        page_1 = []
        page_2 = []
        for index in range(200):
            record = self._chain_record(
                wallet,
                Keypair.random().public_key,
                Asset.native().code,
                "",
                Decimal("1.0000000"),
                "tx-{}".format(index),
                "ignored",
                created_at.isoformat(),
                paging_token="page-1-{}".format(index),
            )
            record["transaction"]["memo_type"] = "none"
            record["transaction"]["memo"] = ""
            page_1.append(record)
        for index in range(200, 400):
            record = self._chain_record(
                wallet,
                Keypair.random().public_key,
                Asset.native().code,
                "",
                Decimal("1.0000000"),
                "tx-{}".format(index),
                "ignored",
                created_at.isoformat(),
                paging_token="page-2-{}".format(index),
            )
            record["transaction"]["memo_type"] = "none"
            record["transaction"]["memo"] = ""
            page_2.append(record)
        server, builder = self._mock_reconcile_server(page_1, page_2, [])

        report = self._reconcile(snapshot_date, snapshot_date, wallet, server=server)

        self.assertGreaterEqual(builder.call.call_count, 3)
        self.assertEqual(len(report.matched), 0)
        self.assertEqual(len(report.chain_only), 0)
        self.assertEqual(len(report.db_only), 0)
        self.assertEqual(len(report.ambiguous), 0)
        self.assertEqual(len(report.missed), 0)

    def test_reconcile_memo_filter_drops_non_bribe_payments(self):
        snapshot_date = timezone.now().date()
        created_at = self._at(snapshot_date, 12)
        wallet = Keypair.random().public_key
        market = self._make_market(
            asset1="AQUA:{}".format(Keypair.random().public_key),
            asset2="sUSD:{}".format(Keypair.random().public_key),
        )
        self._make_day_bribe(
            market,
            snapshot_date,
            asset_code=Asset.native().code,
            asset_issuer="",
        )
        vote = self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "100"
        )
        record_1 = self._chain_record(
            wallet,
            Keypair.random().public_key,
            Asset.native().code,
            "",
            Decimal("1.0000000"),
            "tx-1",
            "",
            created_at.isoformat(),
        )
        record_1["transaction"]["memo_type"] = "none"
        record_2 = self._chain_record(
            wallet,
            Keypair.random().public_key,
            Asset.native().code,
            "",
            Decimal("2.0000000"),
            "tx-2",
            "Hello World",
            created_at.isoformat(),
        )
        record_3 = self._chain_record(
            wallet,
            vote.voting_account,
            Asset.native().code,
            "",
            Decimal("3.0000000"),
            "tx-3",
            "Bribe: {}".format(market.short_value),
            created_at.isoformat(),
        )
        server, _ = self._mock_reconcile_server([record_1, record_2, record_3], [])

        report = self._reconcile(snapshot_date, snapshot_date, wallet, server=server)

        self.assertEqual(len(report.chain_only), 1)
        self.assertEqual(report.chain_only[0][0], "tx-3")
        self.assertEqual(len(report.matched), 0)
        self.assertEqual(len(report.db_only), 0)
        self.assertEqual(len(report.ambiguous), 0)
        self.assertEqual(len(report.missed), 0)

    def _build_reconcile_command_report(self):
        from datetime import date as _date
        from aquarius_bribes.rewards.reconcile import ReconcileReport

        wallet = "GAORXNBAWRIOJ7HRMCTWW2MIB6PYWSC7OKHGIXWTJXYRTZRSHP356TW3"
        from_date = _date(2026, 4, 7)
        to_date = _date(2026, 4, 9)

        return ReconcileReport(
            from_date=from_date,
            to_date=to_date,
            bribe_wallet=wallet,
            chain_payments_count=4,
            db_success_count=3,
            db_failed_count=1,
            matched=[
                (
                    "txhash1",
                    "GAACC1",
                    "AQUA",
                    "GISSUER",
                    Decimal("10.0000000"),
                    1,
                    101,
                    201,
                ),
                (
                    "txhash2",
                    "GAACC2",
                    "AQUA",
                    "GISSUER",
                    Decimal("5.0000000"),
                    1,
                    102,
                    202,
                ),
            ],
            chain_only=[
                (
                    "txhash3",
                    "GAACC3",
                    "AQUA",
                    "GISSUER",
                    Decimal("7.0000000"),
                    1,
                    103,
                    "Bribe: X/Y",
                    None,
                    None,
                ),
            ],
            db_only=[
                (
                    301,
                    "txhash4",
                    "GAACC4",
                    "AQUA",
                    "GISSUER",
                    Decimal("3.0000000"),
                    1,
                    _date(2026, 4, 8),
                ),
            ],
            ambiguous=[],
            missed=[
                (
                    1,
                    501,
                    _date(2026, 4, 8),
                    "GAACC5",
                    "AQUA",
                    "GISSUER",
                    Decimal("2.0000000"),
                ),
                (
                    1,
                    502,
                    _date(2026, 4, 8),
                    "GAACC6",
                    "AQUA",
                    "GISSUER",
                    Decimal("2.0000000"),
                ),
                (
                    1,
                    503,
                    _date(2026, 4, 9),
                    "GAACC7",
                    "AQUA",
                    "GISSUER",
                    Decimal("2.0000000"),
                ),
            ],
            per_bribe_missed={1: {_date(2026, 4, 8): 2, _date(2026, 4, 9): 1}},
        )

    def _reconcile_command_patch(self, report):
        from contextlib import nullcontext
        from importlib.util import find_spec

        module_name = (
            "aquarius_bribes.rewards.management.commands.reconcile_bribe_payouts"
        )
        if find_spec(module_name) is None:
            return nullcontext()
        return mock.patch(
            "{}.reconcile_bribe_payouts".format(module_name),
            return_value=report,
        )

    def _make_report(self, snapshot_date, **kwargs):
        from aquarius_bribes.rewards.reconcile import ReconcileReport

        return ReconcileReport(
            from_date=snapshot_date,
            to_date=snapshot_date,
            bribe_wallet=kwargs.pop("bribe_wallet", Keypair.random().public_key),
            **kwargs,
        )

    def test_reconcile_command_text_output(self):
        report = self._build_reconcile_command_report()
        buf = io.StringIO()

        with self._reconcile_command_patch(report):
            with override_settings(BRIBE_WALLET_ADDRESS="GAORXNBAWRIOJ7HRMCTWW2MIB6PYWSC7OKHGIXWTJXYRTZRSHP356TW3"):
                call_command(
                    "reconcile_bribe_payouts",
                    "--from=2026-04-07",
                    "--to=2026-04-09",
                    stdout=buf,
                )

        output = buf.getvalue()
        self.assertIn("Bucket MATCHED: 2", output)
        self.assertIn("Bucket CHAIN_ONLY: 1", output)
        self.assertIn("Bucket DB_ONLY: 1", output)
        self.assertIn("Bucket MISSED: 3", output)
        self.assertIn("2026-04-07", output)
        self.assertIn("2026-04-09", output)

    def test_fix_db_chain_only_creates_payout(self):
        from aquarius_bribes.rewards.reconcile import apply_fix_db

        snapshot_date = timezone.now().date()
        created_at = self._at(snapshot_date, 12)
        market = self._make_market()
        bribe = self._make_bribe(
            market,
            asset_code=Asset.native().code,
            asset_issuer="",
        )
        vote = self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "100"
        )
        report = self._make_report(
            snapshot_date,
            chain_only=[
                (
                    "tx-create",
                    vote.voting_account,
                    Asset.native().code,
                    "",
                    Decimal("10.0000000"),
                    bribe.id,
                    vote.id,
                    "Bribe: {}".format(market.short_value),
                    created_at,
                    None,
                )
            ],
        )

        before_count = Payout.objects.count()

        counts = apply_fix_db(report)

        self.assertEqual(Payout.objects.count(), before_count + 1)
        payout = Payout.objects.get(stellar_transaction_id="tx-create")
        self.assertEqual(payout.status, Payout.STATUS_SUCCESS)
        self.assertTrue(payout.message.startswith("reconciled "))
        self.assertEqual(counts["chain_only_created"], 1)
        self.assertEqual(counts["chain_only_upgraded"], 0)

    def test_fix_db_chain_only_upgrades_failed_to_success(self):
        from aquarius_bribes.rewards.reconcile import apply_fix_db

        snapshot_date = timezone.now().date()
        created_at = self._at(snapshot_date, 12)
        market = self._make_market()
        bribe = self._make_bribe(
            market,
            asset_code=Asset.native().code,
            asset_issuer="",
        )
        vote = self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "100"
        )
        payout = Payout.objects.create(
            bribe=bribe,
            vote_snapshot=vote,
            stellar_transaction_id="tx-upgrade",
            status=Payout.STATUS_FAILED,
            message="old failure",
            reward_amount=Decimal("11.0000000"),
            asset_code=Asset.native().code,
            asset_issuer="",
        )
        report = self._make_report(
            snapshot_date,
            chain_only=[
                (
                    "tx-upgrade",
                    vote.voting_account,
                    Asset.native().code,
                    "",
                    Decimal("11.0000000"),
                    bribe.id,
                    vote.id,
                    "Bribe: {}".format(market.short_value),
                    created_at,
                    payout.id,
                )
            ],
        )

        counts = apply_fix_db(report)

        payout.refresh_from_db()
        self.assertEqual(Payout.objects.count(), 1)
        self.assertEqual(payout.status, Payout.STATUS_SUCCESS)
        self.assertIn("reconciled", payout.message)
        self.assertEqual(counts["chain_only_created"], 0)
        self.assertEqual(counts["chain_only_upgraded"], 1)

    def test_fix_db_db_only_downgrades_to_failed(self):
        from aquarius_bribes.rewards.reconcile import apply_fix_db

        snapshot_date = timezone.now().date()
        market = self._make_market()
        bribe = self._make_bribe(
            market,
            asset_code=Asset.native().code,
            asset_issuer="",
        )
        vote = self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "100"
        )
        payout = self._make_payout(bribe, vote, "TXHASH", Decimal("12.0000000"))
        report = self._make_report(
            snapshot_date,
            db_only=[
                (
                    payout.id,
                    "TXHASH",
                    vote.voting_account,
                    Asset.native().code,
                    "",
                    Decimal("12.0000000"),
                    bribe.id,
                    snapshot_date,
                )
            ],
        )

        counts = apply_fix_db(report)

        payout.refresh_from_db()
        self.assertEqual(payout.status, Payout.STATUS_FAILED)
        self.assertIn("db_only", payout.message)
        self.assertIn("reconciled", payout.message)
        self.assertEqual(counts["db_only_downgraded"], 1)

    def test_fix_db_idempotent_rerun_no_change(self):
        from aquarius_bribes.rewards.reconcile import apply_fix_db

        snapshot_date = timezone.now().date()
        created_at = self._at(snapshot_date, 12)
        market = self._make_market()
        bribe = self._make_bribe(
            market,
            asset_code=Asset.native().code,
            asset_issuer="",
        )
        create_vote = self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "100"
        )
        downgrade_vote = self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "200"
        )
        downgrade_payout = self._make_payout(
            bribe,
            downgrade_vote,
            "tx-downgrade",
            Decimal("4.0000000"),
        )
        now = timezone.make_aware(datetime.combine(snapshot_date, time(hour=18)))
        report = self._make_report(
            snapshot_date,
            chain_only=[
                (
                    "tx-create-idempotent",
                    create_vote.voting_account,
                    Asset.native().code,
                    "",
                    Decimal("3.0000000"),
                    bribe.id,
                    create_vote.id,
                    "Bribe: {}".format(market.short_value),
                    created_at,
                    None,
                )
            ],
            db_only=[
                (
                    downgrade_payout.id,
                    "tx-downgrade",
                    downgrade_vote.voting_account,
                    Asset.native().code,
                    "",
                    Decimal("4.0000000"),
                    bribe.id,
                    snapshot_date,
                )
            ],
        )

        first_counts = apply_fix_db(report, now=now)
        payout_count = Payout.objects.count()
        second_counts = apply_fix_db(report, now=now)

        self.assertEqual(first_counts["chain_only_created"], 1)
        self.assertEqual(first_counts["db_only_downgraded"], 1)
        self.assertEqual(Payout.objects.count(), payout_count)
        self.assertEqual(second_counts["chain_only_created"], 0)
        self.assertEqual(second_counts["chain_only_upgraded"], 0)
        self.assertEqual(second_counts["db_only_downgraded"], 0)

    def test_fix_db_requires_confirm_without_yes(self):
        snapshot_date = timezone.now().date() - timedelta(days=1)
        report = self._make_report(
            snapshot_date,
            chain_only=[
                (
                    "tx-confirm",
                    Keypair.random().public_key,
                    Asset.native().code,
                    "",
                    Decimal("1.0000000"),
                    1,
                    1,
                    "Bribe: AQUA/XLM",
                    self._at(snapshot_date, 12),
                    None,
                )
            ],
        )
        before_count = Payout.objects.count()

        with mock.patch(
            "aquarius_bribes.rewards.management.commands.reconcile_bribe_payouts.reconcile_bribe_payouts",
            return_value=report,
        ):
            with mock.patch("builtins.input", return_value="n"):
                with mock.patch(
                    "aquarius_bribes.rewards.management.commands.reconcile_bribe_payouts.apply_fix_db"
                ) as mock_apply:
                    with override_settings(BRIBE_WALLET_ADDRESS=Keypair.random().public_key):
                        call_command(
                            "reconcile_bribe_payouts",
                            "--fix-db",
                            "--from={}".format(snapshot_date),
                            "--to={}".format(snapshot_date),
                        )

        mock_apply.assert_not_called()
        self.assertEqual(Payout.objects.count(), before_count)

    def test_fix_db_refuses_today_and_future(self):
        # --fix-db on today or future dates is an unconditional error —
        # task_pay_rewards is still writing Payouts for today, so any
        # reconcile report would go stale between build and apply, risking
        # downgrade of legitimate SUCCESS payouts.
        today = timezone.now().date()
        tomorrow = today + timedelta(days=1)

        for bad_date in (today, tomorrow):
            with self.assertRaisesMessage(CommandError, "--fix-db cannot touch date"):
                with override_settings(BRIBE_WALLET_ADDRESS=Keypair.random().public_key):
                    call_command(
                        "reconcile_bribe_payouts",
                        "--fix-db",
                        "--from={}".format(bad_date),
                        "--to={}".format(bad_date),
                    )

    def test_fix_db_sets_and_clears_active_flag(self):
        from django.core.cache import cache
        from aquarius_bribes.rewards.tasks import PAY_REWARDS_FIX_DB_ACTIVE_KEY

        snapshot_date = timezone.now().date() - timedelta(days=1)
        report = self._make_report(snapshot_date)

        def assert_flag_set(*args, **kwargs):
            self.assertTrue(cache.get(PAY_REWARDS_FIX_DB_ACTIVE_KEY))
            return {
                "chain_only_created": 0,
                "chain_only_upgraded": 0,
                "db_only_downgraded": 0,
                "ambiguous_skipped": 0,
                "chain_orphan_skipped": 0,
                "missed_skipped": 0,
            }

        with mock.patch(
            "aquarius_bribes.rewards.management.commands.reconcile_bribe_payouts.reconcile_bribe_payouts",
            return_value=report,
        ):
            with mock.patch(
                "aquarius_bribes.rewards.management.commands.reconcile_bribe_payouts.apply_fix_db",
                side_effect=assert_flag_set,
            ):
                with mock.patch("builtins.input", return_value="y"):
                    with override_settings(BRIBE_WALLET_ADDRESS=Keypair.random().public_key):
                        call_command(
                            "reconcile_bribe_payouts",
                            "--fix-db",
                            "--from={}".format(snapshot_date),
                            "--to={}".format(snapshot_date),
                        )

        self.assertFalse(cache.get(PAY_REWARDS_FIX_DB_ACTIVE_KEY))

        with mock.patch(
            "aquarius_bribes.rewards.management.commands.reconcile_bribe_payouts.reconcile_bribe_payouts",
            return_value=report,
        ):
            with mock.patch(
                "aquarius_bribes.rewards.management.commands.reconcile_bribe_payouts.apply_fix_db",
                side_effect=RuntimeError("boom"),
            ):
                with mock.patch("builtins.input", return_value="y"):
                    with override_settings(BRIBE_WALLET_ADDRESS=Keypair.random().public_key):
                        with self.assertRaisesMessage(RuntimeError, "boom"):
                            call_command(
                                "reconcile_bribe_payouts",
                                "--fix-db",
                                "--from={}".format(snapshot_date),
                                "--to={}".format(snapshot_date),
                            )

        self.assertFalse(cache.get(PAY_REWARDS_FIX_DB_ACTIVE_KEY))

    def test_fix_db_refuses_when_active_flag_already_held(self):
        # Second operator running --fix-db while another run holds the
        # cache lock must hard-abort — otherwise both runs would enter
        # apply_fix_db and the CHAIN_ONLY create loop could double-insert
        # Payouts. cache.add (not cache.set) makes the guard atomic, and
        # the existing owner's token must stay intact.
        from django.core.cache import cache
        from aquarius_bribes.rewards.tasks import PAY_REWARDS_FIX_DB_ACTIVE_KEY

        snapshot_date = timezone.now().date() - timedelta(days=1)
        report = self._make_report(snapshot_date)

        cache.set(PAY_REWARDS_FIX_DB_ACTIVE_KEY, "other-operator", 60)
        try:
            with mock.patch(
                "aquarius_bribes.rewards.management.commands.reconcile_bribe_payouts.reconcile_bribe_payouts",
                return_value=report,
            ):
                with mock.patch(
                    "aquarius_bribes.rewards.management.commands.reconcile_bribe_payouts.apply_fix_db"
                ) as mock_apply:
                    with mock.patch("builtins.input", return_value="y"):
                        with override_settings(BRIBE_WALLET_ADDRESS=Keypair.random().public_key):
                            with self.assertRaisesMessage(
                                CommandError, "Another --fix-db run is in progress"
                            ):
                                call_command(
                                    "reconcile_bribe_payouts",
                                    "--fix-db",
                                    "--from={}".format(snapshot_date),
                                    "--to={}".format(snapshot_date),
                                )

            mock_apply.assert_not_called()
            # The other operator's token must survive — the refused run
            # never touches a key it does not own.
            self.assertEqual(
                cache.get(PAY_REWARDS_FIX_DB_ACTIVE_KEY), "other-operator"
            )
        finally:
            cache.delete(PAY_REWARDS_FIX_DB_ACTIVE_KEY)

    def test_fix_db_ambiguous_bucket_never_mutates(self):
        from aquarius_bribes.rewards.reconcile import apply_fix_db

        snapshot_date = timezone.now().date()
        market = self._make_market()
        bribe = self._make_bribe(
            market,
            asset_code=Asset.native().code,
            asset_issuer="",
        )
        vote = self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "100"
        )
        payout = self._make_payout(bribe, vote, "tx-existing", Decimal("5.0000000"))
        report = self._make_report(
            snapshot_date,
            ambiguous=[
                (
                    "tx-ambiguous",
                    Keypair.random().public_key,
                    Asset.native().code,
                    "",
                    Decimal("1.0000000"),
                    "Bribe: AQUA/XLM",
                    self._at(snapshot_date, 12),
                    [bribe.id, bribe.id + 1],
                )
            ],
        )

        counts = apply_fix_db(report)

        payout.refresh_from_db()
        self.assertEqual(Payout.objects.count(), 1)
        self.assertEqual(payout.status, Payout.STATUS_SUCCESS)
        self.assertEqual(counts["ambiguous_skipped"], 1)

    def test_fix_db_orphan_chain_payment_never_mutates(self):
        from aquarius_bribes.rewards.reconcile import apply_fix_db

        snapshot_date = timezone.now().date()
        report = self._make_report(
            snapshot_date,
            chain_only=[
                (
                    "tx-orphan",
                    Keypair.random().public_key,
                    Asset.native().code,
                    "",
                    Decimal("1.0000000"),
                    1,
                    None,
                    "Bribe: AQUA/XLM",
                    self._at(snapshot_date, 12),
                    None,
                )
            ],
        )

        counts = apply_fix_db(report)

        self.assertEqual(Payout.objects.count(), 0)
        self.assertEqual(counts["chain_orphan_skipped"], 1)
        self.assertEqual(counts["chain_only_created"], 0)

    def test_completeness_threshold_critical_when_zero_paid(self):
        from aquarius_bribes.rewards.management.commands.check_payout_completeness import (
            run_completeness_check,
        )

        snapshot_date = timezone.now().date()
        market = self._make_market()
        self._make_day_bribe(
            market,
            snapshot_date,
            asset_code=Asset.native().code,
            asset_issuer="",
            total=Decimal("700"),
        )
        for _ in range(3):
            self._make_vote(
                market, Keypair.random().public_key, snapshot_date, "250000"
            )

        result = run_completeness_check(
            date=snapshot_date,
            threshold_pct=5,
            emit_alert=False,
        )

        self.assertEqual(len(result["results"]), 1)
        self.assertEqual(result["results"][0]["severity"], "CRITICAL")
        self.assertTrue(result["has_critical"])
        self.assertFalse(result["has_warning"])

        with self.assertRaises(SystemExit) as exc:
            call_command(
                "check_payout_completeness",
                "--date",
                str(snapshot_date),
                stdout=io.StringIO(),
            )
        self.assertEqual(exc.exception.code, 1)

    def test_completeness_threshold_warning_when_below_threshold(self):
        from aquarius_bribes.rewards.management.commands.check_payout_completeness import (
            run_completeness_check,
        )

        snapshot_date = timezone.now().date()
        market = self._make_market()
        bribe = self._make_day_bribe(
            market,
            snapshot_date,
            asset_code=Asset.native().code,
            asset_issuer="",
            total=Decimal("0.0007000"),
        )
        votes = [
            self._make_vote(
                market, Keypair.random().public_key, snapshot_date, "250000"
            )
            for _ in range(100)
        ]
        for vote in votes[:90]:
            self._make_payout(bribe, vote, "tx-{}".format(vote.id), Decimal("1.0000000"))

        result = run_completeness_check(
            date=snapshot_date,
            threshold_pct=5,
            emit_alert=False,
        )

        self.assertEqual(len(result["results"]), 1)
        self.assertEqual(result["results"][0]["severity"], "WARNING")
        self.assertEqual(result["results"][0]["payable"], 100)
        self.assertEqual(result["results"][0]["paid"], 90)
        self.assertEqual(result["results"][0]["missing"], 10)
        self.assertFalse(result["has_critical"])
        self.assertTrue(result["has_warning"])

        buf = io.StringIO()
        call_command(
            "check_payout_completeness",
            "--date",
            str(snapshot_date),
            stdout=buf,
        )
        self.assertIn("WARNING", buf.getvalue())

    def test_completeness_ignores_delegated_votes(self):
        from aquarius_bribes.rewards.management.commands.check_payout_completeness import (
            run_completeness_check,
        )

        snapshot_date = timezone.now().date()
        market = self._make_market()
        bribe = self._make_day_bribe(
            market,
            snapshot_date,
            asset_code=Asset.native().code,
            asset_issuer="",
        )
        payable_votes = [
            self._make_vote(
                market, Keypair.random().public_key, snapshot_date, "250000"
            )
            for _ in range(2)
        ]
        for _ in range(3):
            self._make_vote(
                market,
                Keypair.random().public_key,
                snapshot_date,
                "250000",
                has_delegation=True,
            )
        for vote in payable_votes:
            self._make_payout(bribe, vote, "tx-{}".format(vote.id), Decimal("1.0000000"))

        result = run_completeness_check(
            date=snapshot_date,
            threshold_pct=5,
            emit_alert=False,
        )

        self.assertEqual(result["results"][0]["payable"], 2)
        self.assertEqual(result["results"][0]["paid"], 2)
        self.assertEqual(result["results"][0]["severity"], "OK")

    def test_completeness_ignores_accounts_without_trustline_for_non_native_bribe(
        self,
    ):
        from aquarius_bribes.rewards.management.commands.check_payout_completeness import (
            run_completeness_check,
        )

        snapshot_date = timezone.now().date()
        issuer = Keypair.random().public_key
        market = self._make_market()
        bribe = self._make_day_bribe(
            market,
            snapshot_date,
            asset_code="USDC",
            asset_issuer=issuer,
        )
        votes = [
            self._make_vote(
                market, Keypair.random().public_key, snapshot_date, "250000"
            )
            for _ in range(4)
        ]
        for vote in votes[:2]:
            self._make_holder(vote.voting_account, "USDC", issuer, self._at(snapshot_date, 12))
            Payout.objects.create(
                bribe=bribe,
                vote_snapshot=vote,
                stellar_transaction_id="tx-{}".format(vote.id),
                status=Payout.STATUS_SUCCESS,
                reward_amount=Decimal("1.0000000"),
                asset_code="USDC",
                asset_issuer=issuer,
            )

        result = run_completeness_check(
            date=snapshot_date,
            threshold_pct=5,
            emit_alert=False,
        )

        self.assertEqual(result["results"][0]["payable"], 2)
        self.assertEqual(result["results"][0]["paid"], 2)
        self.assertEqual(result["results"][0]["severity"], "OK")

    def test_completeness_ignores_dust_votes_below_min_threshold(self):
        from aquarius_bribes.rewards.management.commands.check_payout_completeness import (
            run_completeness_check,
        )

        snapshot_date = timezone.now().date()
        market = self._make_market()
        bribe = self._make_day_bribe(
            market,
            snapshot_date,
            asset_code=Asset.native().code,
            asset_issuer="",
            total=Decimal("0.0007000"),
        )
        regular_votes, _dust_vote = self._make_dust_votes(market, snapshot_date)
        for vote in regular_votes:
            self._make_payout(bribe, vote, "tx-{}".format(vote.id), Decimal("25.0000000"))

        result = run_completeness_check(
            date=snapshot_date,
            threshold_pct=5,
            emit_alert=False,
        )

        self.assertEqual(result["results"][0]["payable"], 4)
        self.assertEqual(result["results"][0]["paid"], 4)
        self.assertEqual(result["results"][0]["severity"], "OK")

    def test_completeness_task_retries_when_pay_rewards_flag_set(self):
        from aquarius_bribes.rewards.tasks import (
            PAY_REWARDS_TASK_ACTIVE_KEY,
            task_check_payout_completeness,
        )

        def fake_cache_get(key, default=False):
            if key == PAY_REWARDS_TASK_ACTIVE_KEY:
                return True
            return False

        with mock.patch(
            "aquarius_bribes.rewards.tasks.cache.get", side_effect=fake_cache_get
        ):
            with mock.patch.object(task_check_payout_completeness, "retry") as mock_retry:
                mock_retry.return_value = None
                task_check_payout_completeness.run()

        mock_retry.assert_called_once()
        # Retry must thread the frozen snapshot_date through so a retry
        # crossing UTC midnight still checks the date the task was
        # originally dispatched for (X6).
        retry_kwargs = mock_retry.call_args.kwargs
        self.assertIn("args", retry_kwargs)
        (snapshot_date_iso,) = retry_kwargs["args"]
        expected = (timezone.now().date() - timedelta(days=1)).isoformat()
        self.assertEqual(snapshot_date_iso, expected)

    def test_completeness_task_uses_frozen_date_across_retries(self):
        # On a retry dispatched after UTC midnight, the task must still
        # operate on the original snapshot_date passed through retry args,
        # not recompute (today - 1) from the new wall-clock. Without this,
        # a 01:00 UTC task for "yesterday" delayed past midnight would
        # silently check the previous "today" (still being written).
        from aquarius_bribes.rewards.tasks import task_check_payout_completeness

        original_snapshot_date = date(2026, 4, 19)

        with mock.patch(
            "aquarius_bribes.rewards.tasks.cache.get", return_value=False
        ):
            with mock.patch(
                "aquarius_bribes.rewards.management.commands"
                ".check_payout_completeness.run_completeness_check"
            ) as mock_run:
                task_check_payout_completeness.run(
                    snapshot_date_iso=original_snapshot_date.isoformat()
                )

        mock_run.assert_called_once()
        self.assertEqual(mock_run.call_args.kwargs["date"], original_snapshot_date)

    def test_completeness_task_emits_warning_when_retries_exhausted(self):
        from celery.exceptions import MaxRetriesExceededError
        from aquarius_bribes.rewards.tasks import (
            PAY_REWARDS_TASK_ACTIVE_KEY,
            task_check_payout_completeness,
        )

        def fake_cache_get(key, default=False):
            if key == PAY_REWARDS_TASK_ACTIVE_KEY:
                return True
            return False

        with mock.patch(
            "aquarius_bribes.rewards.tasks.cache.get", side_effect=fake_cache_get
        ):
            with mock.patch.object(
                task_check_payout_completeness,
                "retry",
                side_effect=MaxRetriesExceededError("retries exhausted"),
            ):
                with mock.patch("sentry_sdk.capture_message") as mock_capture:
                    task_check_payout_completeness.run()

        mock_capture.assert_called_once()
        self.assertIn("completeness-check-skipped", mock_capture.call_args[0][0])
        self.assertEqual(mock_capture.call_args[1]["level"], "warning")

    @override_settings(
        PAYOUT_COMPLETENESS_ALERT_ENABLED=True,
        PAYOUT_COMPLETENESS_THRESHOLD_PCT=5,
    )
    def test_completeness_task_emits_sentry_when_setting_enabled(self):
        from aquarius_bribes.rewards.tasks import task_check_payout_completeness

        snapshot_date = timezone.now().date() - timedelta(days=1)
        market = self._make_market()
        self._make_day_bribe(
            market,
            snapshot_date,
            asset_code=Asset.native().code,
            asset_issuer="",
        )
        self._make_vote(market, Keypair.random().public_key, snapshot_date, "250000")

        with mock.patch(
            "aquarius_bribes.rewards.tasks.cache.get", return_value=False
        ):
            with mock.patch(
                "aquarius_bribes.rewards.management.commands.check_payout_completeness.sentry_sdk.capture_message"
            ) as mock_capture:
                task_check_payout_completeness.run()

        mock_capture.assert_called_once()
        self.assertEqual(mock_capture.call_args[1]["level"], "error")

    def test_completeness_task_does_not_block_on_load_votes_or_load_trustors_flags(
        self,
    ):
        from aquarius_bribes.rewards.tasks import (
            LOAD_TRUSTORS_TASK_ACTIVE_KEY,
            LOAD_VOTES_TASK_ACTIVE_KEY,
            PAY_REWARDS_FIX_DB_ACTIVE_KEY,
            PAY_REWARDS_TASK_ACTIVE_KEY,
            task_check_payout_completeness,
        )

        def fake_cache_get(key, default=False):
            if key == LOAD_VOTES_TASK_ACTIVE_KEY:
                return True
            if key == LOAD_TRUSTORS_TASK_ACTIVE_KEY:
                return True
            if key == PAY_REWARDS_FIX_DB_ACTIVE_KEY:
                return False
            if key == PAY_REWARDS_TASK_ACTIVE_KEY:
                return False
            return default

        with mock.patch(
            "aquarius_bribes.rewards.tasks.cache.get", side_effect=fake_cache_get
        ):
            with mock.patch.object(
                task_check_payout_completeness,
                "retry",
                side_effect=AssertionError("retry should not be called"),
            ):
                with mock.patch(
                    "aquarius_bribes.rewards.management.commands.check_payout_completeness.run_completeness_check",
                    return_value={"results": [], "has_critical": False, "has_warning": False},
                ) as mock_run:
                    task_check_payout_completeness.run()

        mock_run.assert_called_once()

    def test_pay_reward_build_failure_exits_without_infinite_loop(self):
        # Within a single pay_reward run, a persistent build_failure must not
        # re-enqueue the same page forever even though _clean_rewards now
        # treats build_failure: payouts as retryable across runs.
        market = self._make_market()
        bribe = self._make_bribe(market)
        snapshot_date = timezone.now().date()
        for _ in range(3):
            self._make_vote(
                market, Keypair.random().public_key, snapshot_date, "100"
            )
        wallet = SecuredWallet(
            public_key=Keypair.random().public_key, secret=None
        )
        asset = Asset.native()
        payer = RewardPayer(bribe, wallet, asset, bribe.total_reward_amount)
        votes = VoteSnapshot.objects.filter(
            market_key=market, snapshot_time=snapshot_date
        )

        with mock.patch.object(
            payer, "_build_transaction",
            side_effect=RuntimeError("horizon upstream slow"),
        ) as mock_build:
            payer.pay_reward(votes)

        self.assertEqual(mock_build.call_count, 1)
        self.assertEqual(
            Payout.objects.filter(
                bribe=bribe, status=Payout.STATUS_FAILED,
                message__startswith="build_failure:",
            ).count(),
            3,
        )

    def test_clean_rewards_retries_build_failure_payouts(self):
        market = self._make_market()
        bribe = self._make_bribe(market)
        snapshot_date = timezone.now().date()
        vote = self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "100"
        )
        Payout.objects.create(
            bribe=bribe,
            vote_snapshot=vote,
            asset_code=bribe.asset_code,
            asset_issuer=bribe.asset_issuer or "",
            reward_amount=Decimal("1.0000000"),
            stellar_transaction_id="",
            status=Payout.STATUS_FAILED,
            message="build_failure: TimeoutError: horizon upstream slow",
        )
        wallet = SecuredWallet(
            public_key=Keypair.random().public_key, secret=None
        )
        asset = (
            Asset.native()
            if bribe.asset_code == "native"
            else Asset(bribe.asset_code, bribe.asset_issuer)
        )
        payer = RewardPayer(bribe, wallet, asset, bribe.total_reward_amount)
        remaining = payer._clean_rewards(
            VoteSnapshot.objects.filter(
                market_key=market, snapshot_time=snapshot_date
            )
        )
        self.assertIn(vote, list(remaining))

    def test_clean_rewards_still_excludes_terminal_failed_payouts(self):
        # Regression guard: op_no_trust remains terminal and the build_failure
        # carve-out must not re-open it.
        market = self._make_market()
        bribe = self._make_bribe(market)
        snapshot_date = timezone.now().date()
        vote = self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "100"
        )
        Payout.objects.create(
            bribe=bribe,
            vote_snapshot=vote,
            asset_code=bribe.asset_code,
            asset_issuer=bribe.asset_issuer or "",
            reward_amount=Decimal("1.0000000"),
            stellar_transaction_id="deadbeef",
            status=Payout.STATUS_FAILED,
            message="op_no_trust",
        )
        wallet = SecuredWallet(
            public_key=Keypair.random().public_key, secret=None
        )
        asset = (
            Asset.native()
            if bribe.asset_code == "native"
            else Asset(bribe.asset_code, bribe.asset_issuer)
        )
        payer = RewardPayer(bribe, wallet, asset, bribe.total_reward_amount)
        remaining = payer._clean_rewards(
            VoteSnapshot.objects.filter(
                market_key=market, snapshot_time=snapshot_date
            )
        )
        self.assertNotIn(vote, list(remaining))

    def test_reconcile_pagination_exits_before_exhausting_history(self):
        # Performance regression guard: desc-order walk must early-exit once the
        # page crosses below from_date, without fetching further pages.
        from aquarius_bribes.rewards.reconcile import _iter_payment_ops

        from_d = datetime(2026, 4, 8).date()
        to_d = datetime(2026, 4, 8).date()
        wallet = Keypair.random().public_key

        future_record = self._chain_record(
            bribe_wallet=wallet,
            to=Keypair.random().public_key,
            asset_code="XLM",
            asset_issuer="",
            amount="1.0000000",
            tx_hash="future_tx",
            memo="Bribe: ignored",
            created_at_str="2026-04-09T00:00:00Z",
            paging_token="tok_future",
        )
        in_window_record = self._chain_record(
            bribe_wallet=wallet,
            to=Keypair.random().public_key,
            asset_code="XLM",
            asset_issuer="",
            amount="1.0000000",
            tx_hash="window_tx",
            memo="Bribe: window",
            created_at_str="2026-04-08T12:00:00Z",
            paging_token="tok_window",
        )
        ancient_record = self._chain_record(
            bribe_wallet=wallet,
            to=Keypair.random().public_key,
            asset_code="XLM",
            asset_issuer="",
            amount="1.0000000",
            tx_hash="ancient_tx",
            memo="Bribe: ancient",
            created_at_str="2026-04-07T00:00:00Z",
            paging_token="tok_ancient",
        )

        server, builder = self._mock_reconcile_server(
            [future_record],
            [in_window_record],
            [ancient_record],
            [
                self._chain_record(
                    bribe_wallet=wallet,
                    to=Keypair.random().public_key,
                    asset_code="XLM",
                    asset_issuer="",
                    amount="1.0000000",
                    tx_hash="should_not_fetch",
                    memo="Bribe: unreachable",
                    created_at_str="2022-01-01T00:00:00Z",
                    paging_token="tok_unreachable",
                )
            ],
        )

        yielded = list(_iter_payment_ops(server, wallet, from_d, to_d))

        self.assertEqual(len(yielded), 1)
        self.assertEqual(yielded[0][0]["transaction_hash"], "window_tx")
        # Newest-first walk: order must be called with desc=True.
        builder.order.assert_called_with(desc=True)
        # Must stop after 3 pages (future skipped, window yielded, ancient triggers return).
        self.assertEqual(builder.call.call_count, 3)

    def test_fix_db_aborts_when_lock_lost_mid_run(self):
        # X1: if the cache lock is lost mid-run (TTL expiry or takeover by
        # another operator), apply_fix_db must raise FixDbLockLost so the
        # CLI can surface it as a CommandError and stop further mutations.
        from django.core.cache import cache
        from aquarius_bribes.rewards.reconcile import FixDbLockLost, apply_fix_db
        from aquarius_bribes.rewards.tasks import PAY_REWARDS_FIX_DB_ACTIVE_KEY

        snapshot_date = timezone.now().date() - timedelta(days=1)
        market = self._make_market()
        bribe = self._make_bribe(market, asset_code=Asset.native().code, asset_issuer="")
        # Build 120 chain_only items so the touch fires at rows 50 AND 100 —
        # we need at least two touch boundaries for patched_get to return
        # owner_token first (row 50) and other_token second (row 100).
        chain_only_items = []
        for _ in range(120):
            vote = self._make_vote(market, Keypair.random().public_key, snapshot_date, "100")
            chain_only_items.append((
                "tx-{}".format(vote.id),
                vote.voting_account,
                Asset.native().code,
                "",
                Decimal("1.0000000"),
                bribe.id,
                vote.id,
                "Bribe: X/Y",
                self._at(snapshot_date, 12),
                None,
            ))

        report = self._make_report(snapshot_date, chain_only=chain_only_items)
        owner_token = "real-owner"
        other_token = "other-operator"
        call_count = [0]

        original_get = cache.get

        def patched_get(key, *args, **kwargs):
            if key == PAY_REWARDS_FIX_DB_ACTIVE_KEY:
                call_count[0] += 1
                # First call returns real owner; second returns different token
                # to simulate lock takeover.
                if call_count[0] == 1:
                    return owner_token
                return other_token
            return original_get(key, *args, **kwargs)

        with mock.patch("aquarius_bribes.rewards.reconcile.cache.get", side_effect=patched_get):
            with mock.patch("aquarius_bribes.rewards.reconcile.cache.touch"):
                with self.assertRaises(FixDbLockLost):
                    apply_fix_db(
                        report,
                        now=timezone.now(),
                        cache_lock_key=PAY_REWARDS_FIX_DB_ACTIVE_KEY,
                        owner_token=owner_token,
                        touch_every=50,
                    )

    def test_fix_db_aborts_surfaces_as_command_error(self):
        # Verify FixDbLockLost from apply_fix_db is caught by the CLI and
        # re-raised as CommandError.
        from aquarius_bribes.rewards.reconcile import FixDbLockLost

        snapshot_date = timezone.now().date() - timedelta(days=1)
        report = self._make_report(snapshot_date)

        with mock.patch(
            "aquarius_bribes.rewards.management.commands.reconcile_bribe_payouts.reconcile_bribe_payouts",
            return_value=report,
        ):
            with mock.patch(
                "aquarius_bribes.rewards.management.commands.reconcile_bribe_payouts.apply_fix_db",
                side_effect=FixDbLockLost("lock gone"),
            ):
                with mock.patch("builtins.input", return_value="y"):
                    with override_settings(BRIBE_WALLET_ADDRESS=Keypair.random().public_key):
                        with self.assertRaises(CommandError):
                            call_command(
                                "reconcile_bribe_payouts",
                                "--fix-db",
                                "--from={}".format(snapshot_date),
                                "--to={}".format(snapshot_date),
                            )

    def test_fix_db_touches_lock_every_n_rows(self):
        # X1: cache.touch must be called at least once every touch_every rows.
        from aquarius_bribes.rewards.reconcile import apply_fix_db
        from aquarius_bribes.rewards.tasks import PAY_REWARDS_FIX_DB_ACTIVE_KEY

        snapshot_date = timezone.now().date() - timedelta(days=1)
        market = self._make_market()
        bribe = self._make_bribe(market, asset_code=Asset.native().code, asset_issuer="")
        chain_only_items = []
        for _ in range(150):
            vote = self._make_vote(market, Keypair.random().public_key, snapshot_date, "100")
            chain_only_items.append((
                "tx-{}".format(vote.id),
                vote.voting_account,
                Asset.native().code,
                "",
                Decimal("1.0000000"),
                bribe.id,
                vote.id,
                "Bribe: X/Y",
                self._at(snapshot_date, 12),
                None,
            ))

        owner_token = "my-token"
        report = self._make_report(snapshot_date, chain_only=chain_only_items)

        with mock.patch("aquarius_bribes.rewards.reconcile.cache.touch") as mock_touch:
            with mock.patch(
                "aquarius_bribes.rewards.reconcile.cache.get", return_value=owner_token
            ):
                apply_fix_db(
                    report,
                    now=timezone.now(),
                    cache_lock_key=PAY_REWARDS_FIX_DB_ACTIVE_KEY,
                    owner_token=owner_token,
                    touch_every=50,
                )

        # 150 rows / 50 = 3 touches expected.
        self.assertGreaterEqual(mock_touch.call_count, 3)
        # Each touch must use the correct key and TTL.
        for call in mock_touch.call_args_list:
            self.assertEqual(call.args[0], PAY_REWARDS_FIX_DB_ACTIVE_KEY)

    def test_missed_walk_uses_single_query_per_bribe_per_day(self):
        # X3: the MISSED walk must issue exactly one Payout lookup per
        # (bribe, day) rather than one per vote. Use assertNumQueries to
        # verify the count stays constant as vote count grows.
        from django.test.utils import CaptureQueriesContext
        from django.db import connection

        snapshot_date = timezone.now().date()
        wallet = Keypair.random().public_key
        market = self._make_market()
        self._make_bribe(
            market,
            asset_code=Asset.native().code,
            asset_issuer="",
        )
        # Create 10 votes — without the hoist this would cause 10 per-vote
        # Payout queries; with the hoist it is exactly 1 per bribe per day.
        for _ in range(10):
            self._make_vote(market, Keypair.random().public_key, snapshot_date, "100")

        server, _ = self._mock_reconcile_server([], [])

        with CaptureQueriesContext(connection) as ctx:
            report = self._reconcile(snapshot_date, snapshot_date, wallet, server=server)

        # The MISSED-walk hoisted query is uniquely identified by filtering
        # on bribe_id = X and snapshot_time = D (scalar equality). The other
        # Payout queries in reconcile_bribe_payouts use snapshot_time BETWEEN
        # and carry no bribe_id predicate, so excluding BETWEEN narrows the
        # match to exactly the query we care about.
        missed_walk_queries = [
            q for q in ctx.captured_queries
            if "rewards_payout" in q["sql"].lower()
            and "bribe_id" in q["sql"].lower()
            and "snapshot_time" in q["sql"].lower()
            and "between" not in q["sql"].lower()
        ]
        # Should be exactly 1 (hoisted per-bribe-per-day query), not 10.
        self.assertEqual(len(missed_walk_queries), 1)
        self.assertEqual(len(report.missed), 10)

    def test_task_pay_rewards_reuses_asset_holder_cache(self):
        # X4: get_payable_votes must be called with the same asset_holder_cache
        # dict across bribes — verifiable by checking the cache grows across calls
        # rather than starting fresh each time.
        from aquarius_bribes.rewards.eligibility import get_payable_votes as real_gpv

        snapshot_date = timezone.now().date()
        market1 = self._make_market()
        market2 = self._make_market()
        issuer = Keypair.random().public_key
        asset_code = "AQUA"

        for market in (market1, market2):
            AggregatedByAssetBribe.objects.create(
                market_key=market,
                asset_code=asset_code,
                asset_issuer=issuer,
                start_at=timezone.now() - timedelta(hours=1),
                stop_at=timezone.now() + timedelta(hours=1),
                total_reward_amount=Decimal("700"),
            )
            self._make_vote(market, Keypair.random().public_key, snapshot_date, "1000")
            self._make_holder(
                Keypair.random().public_key, asset_code, issuer, self._at(snapshot_date, 12)
            )

        calls = []

        def spy_gpv(bribe, snap_date, reward_amount=None, asset_holder_cache=None):
            calls.append(asset_holder_cache)
            return real_gpv(bribe, snap_date, reward_amount=reward_amount, asset_holder_cache=asset_holder_cache)

        with mock.patch("aquarius_bribes.rewards.tasks.get_payable_votes", side_effect=spy_gpv):
            with mock.patch("aquarius_bribes.rewards.tasks.SecuredWallet"):
                with mock.patch("aquarius_bribes.rewards.tasks.RewardPayer"):
                    task_pay_rewards()

        # All calls must have received the same cache dict object (not None).
        self.assertGreaterEqual(len(calls), 2)
        self.assertIsNotNone(calls[0])
        # Verify they're all the same dict instance — cache is shared.
        first_cache = calls[0]
        for c in calls[1:]:
            self.assertIs(c, first_cache)

    def test_chain_only_weekly_rollover_not_ambiguous(self):
        # X5: two back-to-back weekly bribes with the same (asset, market_key).
        # A chain op on Monday 00:01 for week-N snapshot must resolve to bribe N,
        # NOT be flagged AMBIGUOUS because bribe N+1 also matches a naive ±1 day
        # candidate window.
        # Week N: ends Monday 00:00; Week N+1: starts Monday 00:00.
        monday = timezone.now().date() - timedelta(days=7)
        # Adjust to the most recent Monday
        monday = monday - timedelta(days=monday.weekday())
        snapshot_date = monday - timedelta(days=1)  # Sunday = last day of week N
        wallet = Keypair.random().public_key
        market = self._make_market(
            asset1="AQUA:{}".format(Keypair.random().public_key),
            asset2="native",
        )
        monday_aware = timezone.make_aware(datetime.combine(monday, time(0, 0, 0)))
        # Bribe N: active until Monday 00:00
        bribe_n = AggregatedByAssetBribe.objects.create(
            market_key=market,
            asset_code=Asset.native().code,
            asset_issuer="",
            start_at=monday_aware - timedelta(days=7),
            stop_at=monday_aware,
            total_reward_amount=Decimal("700"),
        )
        # Bribe N+1: starts Monday 00:00
        AggregatedByAssetBribe.objects.create(
            market_key=market,
            asset_code=Asset.native().code,
            asset_issuer="",
            start_at=monday_aware,
            stop_at=monday_aware + timedelta(days=7),
            total_reward_amount=Decimal("700"),
        )
        vote = self._make_vote(market, Keypair.random().public_key, snapshot_date, "100")
        # Chain op at Monday 00:01 — crosses into Monday but vote was Sunday.
        created_at = timezone.make_aware(datetime.combine(monday, time(0, 1, 0)))
        server, _ = self._mock_reconcile_server(
            [
                self._chain_record(
                    wallet,
                    vote.voting_account,
                    Asset.native().code,
                    "",
                    Decimal("10.0000000"),
                    "tx-rollover",
                    "Bribe: {}".format(market.short_value),
                    created_at.isoformat(),
                ),
            ],
            [],
        )

        # Reconcile with window covering snapshot_date (Sunday).
        report = self._reconcile(snapshot_date, snapshot_date, wallet, server=server)

        # Must not be AMBIGUOUS — VoteSnapshot-first resolution picks bribe N.
        self.assertEqual(len(report.ambiguous), 0)
        # Should land in chain_only (no Payout yet) resolved to bribe N,
        # or skipped_no_active_bribe if the VoteSnapshot resolves cleanly.
        # The key assertion is: no ambiguity.
        chain_only_bribe_ids = {row[5] for row in report.chain_only if row[6] is not None}
        self.assertNotIn(None, chain_only_bribe_ids)
        if chain_only_bribe_ids:
            self.assertIn(bribe_n.id, chain_only_bribe_ids)

    def test_chain_only_resolves_via_vote_snapshot_date(self):
        # X5: two bribes with overlapping asset+market_key but different active
        # windows. A chain op inside only bribe A's window must resolve to A,
        # not B, after VoteSnapshot-first resolution.
        snapshot_date = timezone.now().date() - timedelta(days=3)
        wallet = Keypair.random().public_key
        market = self._make_market(
            asset1="AQUA:{}".format(Keypair.random().public_key),
            asset2="native",
        )
        # Bribe A: active around snapshot_date
        bribe_a = AggregatedByAssetBribe.objects.create(
            market_key=market,
            asset_code=Asset.native().code,
            asset_issuer="",
            start_at=self._at(snapshot_date, 0) - timedelta(days=1),
            stop_at=self._at(snapshot_date, 0) + timedelta(days=2),
            total_reward_amount=Decimal("700"),
        )
        # Bribe B: starts after snapshot_date — should NOT match a vote on snapshot_date.
        AggregatedByAssetBribe.objects.create(
            market_key=market,
            asset_code=Asset.native().code,
            asset_issuer="",
            start_at=self._at(snapshot_date, 0) + timedelta(days=3),
            stop_at=self._at(snapshot_date, 0) + timedelta(days=10),
            total_reward_amount=Decimal("700"),
        )
        vote = self._make_vote(market, Keypair.random().public_key, snapshot_date, "100")
        created_at = self._at(snapshot_date, 12)
        server, _ = self._mock_reconcile_server(
            [
                self._chain_record(
                    wallet,
                    vote.voting_account,
                    Asset.native().code,
                    "",
                    Decimal("10.0000000"),
                    "tx-a-only",
                    "Bribe: {}".format(market.short_value),
                    created_at.isoformat(),
                ),
            ],
            [],
        )

        report = self._reconcile(snapshot_date, snapshot_date, wallet, server=server)

        self.assertEqual(len(report.ambiguous), 0)
        chain_only_with_vs = [row for row in report.chain_only if row[6] is not None]
        self.assertEqual(len(chain_only_with_vs), 1)
        self.assertEqual(chain_only_with_vs[0][5], bribe_a.id)

    def test_unknown_response_failed_payouts_remain_retryable(self):
        # X6: a failed Payout with message='unknown_response_no_successful_field'
        # must be included in the retryable set — the next _clean_rewards call
        # must NOT exclude it, so task_pay_rewards will retry on the next run.
        market = self._make_market()
        bribe = self._make_bribe(market, asset_code=Asset.native().code, asset_issuer="")
        snapshot_date = timezone.now().date()
        vote = self._make_vote(market, Keypair.random().public_key, snapshot_date, "100")
        Payout.objects.create(
            bribe=bribe,
            vote_snapshot=vote,
            asset_code=bribe.asset_code,
            asset_issuer=bribe.asset_issuer or "",
            reward_amount=Decimal("1.0000000"),
            stellar_transaction_id="deadbeef",
            status=Payout.STATUS_FAILED,
            message="unknown_response_no_successful_field",
        )
        wallet = SecuredWallet(public_key=Keypair.random().public_key, secret=None)
        asset = Asset.native()
        payer = RewardPayer(bribe, wallet, asset, bribe.total_reward_amount)
        remaining = payer._clean_rewards(
            VoteSnapshot.objects.filter(market_key=market, snapshot_time=snapshot_date)
        )
        # Vote must still be in the eligible set (retryable, not permanently excluded).
        self.assertIn(vote, list(remaining))

    def test_clean_failed_payouts_rechecks_unknown_response_under_5_minutes(self):
        # X1 (2026-04-24 audit): bypass the 5-min timeout grace for
        # `unknown_response_no_successful_field` so an operator's manual
        # re-run during incident response re-verifies the hash before
        # _clean_rewards exposes the voter to a fresh submit (which would
        # double-pay if the original tx actually landed).
        market = self._make_market()
        bribe = self._make_bribe(
            market, asset_code=Asset.native().code, asset_issuer=""
        )
        snapshot_date = timezone.now().date()
        vote = self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "100"
        )
        payout = Payout.objects.create(
            bribe=bribe,
            vote_snapshot=vote,
            asset_code=bribe.asset_code,
            asset_issuer=bribe.asset_issuer or "",
            reward_amount=Decimal("1.0000000"),
            stellar_transaction_id="landedhash",
            status=Payout.STATUS_FAILED,
            message="unknown_response_no_successful_field",
        )
        # Payout is fresh (<5 min old); the old code path would skip it.
        tx_call = mock.MagicMock()
        tx_call.call = mock.MagicMock(return_value={"successful": True})
        transactions_builder = mock.MagicMock()
        transactions_builder.transaction = mock.MagicMock(return_value=tx_call)
        mock_server = mock.MagicMock()
        mock_server.transactions = mock.MagicMock(return_value=transactions_builder)

        wallet = SecuredWallet(public_key="G" + "A" * 55, secret="S" + "A" * 55)
        payer = RewardPayer(bribe, wallet, bribe.asset, bribe.total_reward_amount)
        payer.server = mock_server

        payer._clean_failed_payouts(
            VoteSnapshot.objects.filter(market_key=market, snapshot_time=snapshot_date)
        )

        transactions_builder.transaction.assert_called_once_with("landedhash")
        payout.refresh_from_db()
        self.assertEqual(payout.status, Payout.STATUS_SUCCESS)

    def test_clean_failed_payouts_keeps_timeout_5_minute_grace(self):
        # X1 (2026-04-24 audit) regression: `timeout` retains the 5-min
        # grace so the re-check doesn't race Horizon's own indexing lag
        # on a tx that is still mid-landing.
        market = self._make_market()
        bribe = self._make_bribe(
            market, asset_code=Asset.native().code, asset_issuer=""
        )
        snapshot_date = timezone.now().date()
        vote = self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "100"
        )
        payout = Payout.objects.create(
            bribe=bribe,
            vote_snapshot=vote,
            asset_code=bribe.asset_code,
            asset_issuer=bribe.asset_issuer or "",
            reward_amount=Decimal("1.0000000"),
            stellar_transaction_id="pendinghash",
            status=Payout.STATUS_FAILED,
            message="timeout",
        )
        mock_server = mock.MagicMock()
        mock_server.transactions = mock.MagicMock()

        wallet = SecuredWallet(public_key="G" + "A" * 55, secret="S" + "A" * 55)
        payer = RewardPayer(bribe, wallet, bribe.asset, bribe.total_reward_amount)
        payer.server = mock_server

        payer._clean_failed_payouts(
            VoteSnapshot.objects.filter(market_key=market, snapshot_time=snapshot_date)
        )

        mock_server.transactions.assert_not_called()
        payout.refresh_from_db()
        self.assertEqual(payout.status, Payout.STATUS_FAILED)
        self.assertEqual(payout.message, "timeout")

    def test_chain_only_multi_day_range_resolves_to_chain_date_snapshot(self):
        # X2 (2026-04-24 audit) + X5 iter-3 refinement: a voter with
        # VoteSnapshots on both D-1 and D inside a multi-day reconcile range
        # must not be routed to AMBIGUOUS when memo+asset+short_value+amount
        # uniquely identifies the (bribe, VS) pair. The resolver does amount-
        # replay per-date: the chain op amount matches exactly one date's
        # expected, so that date's VS is returned.
        wallet = Keypair.random().public_key
        today = timezone.now().date()
        d_prev = today - timedelta(days=3)
        d_chain = today - timedelta(days=2)
        market = self._make_market()
        bribe = self._make_bribe(
            market,
            asset_code=Asset.native().code,
            asset_issuer="",
            start=self._at(d_prev, 0),
            stop=self._at(d_chain, 0) + timedelta(days=1),
        )
        voter = Keypair.random().public_key
        # Different total_votes on each day so the per-day expected amounts
        # differ — otherwise a single-voter-on-both-days setup produces
        # identical expected amounts and the amount-match can't pick one.
        self._make_vote(market, voter, d_prev, "100")
        self._make_vote(market, Keypair.random().public_key, d_prev, "300")
        vote_chain = self._make_vote(market, voter, d_chain, "100")

        created_at = self._at(d_chain, 12)
        # Amount matches vote_chain on d_chain exclusively: d_chain has only
        # this voter so expected = daily * 100/100 = daily; d_prev has total
        # 400 so expected for vote_prev = daily * 100/400 = daily/4.
        expected_amount = Decimal(bribe.daily_amount).quantize(
            Decimal("0.0000001"), rounding=ROUND_DOWN,
        )
        server, _ = self._mock_reconcile_server(
            [
                self._chain_record(
                    wallet,
                    voter,
                    Asset.native().code,
                    "",
                    expected_amount,
                    "tx-mday",
                    "Bribe: {}".format(market.short_value),
                    created_at.isoformat(),
                ),
            ],
            [],
        )

        report = self._reconcile(d_prev, d_chain, wallet, server=server)

        self.assertEqual(len(report.ambiguous), 0)
        chain_only_with_vs = [row for row in report.chain_only if row[6] is not None]
        self.assertEqual(len(chain_only_with_vs), 1)
        self.assertEqual(chain_only_with_vs[0][5], bribe.id)
        self.assertEqual(chain_only_with_vs[0][6], vote_chain.id)

    def test_chain_only_multi_day_range_midnight_cross_falls_back_to_prior_day(self):
        # X2 (2026-04-24 audit): if the voter has no VS on `chain_date`
        # (midnight-cross: tx submitted for snapshot_time=D-1 but landed on
        # D 00:0x), the resolver must still succeed by falling back to the
        # only remaining candidate date (D-1).
        wallet = Keypair.random().public_key
        today = timezone.now().date()
        d_prev = today - timedelta(days=3)
        d_chain = today - timedelta(days=2)
        market = self._make_market()
        bribe = self._make_bribe(
            market,
            asset_code=Asset.native().code,
            asset_issuer="",
            start=self._at(d_prev, 0),
            stop=self._at(d_chain, 0),  # active only on D-1
        )
        voter = Keypair.random().public_key
        vote_prev = self._make_vote(market, voter, d_prev, "100")

        created_at = self._at(d_chain, 0) + timedelta(minutes=1)
        expected_amount = (
            Decimal(bribe.daily_amount)
            * Decimal(vote_prev.votes_value)
            / Decimal(vote_prev.votes_value)
        ).quantize(Decimal("0.0000001"), rounding=ROUND_DOWN)
        server, _ = self._mock_reconcile_server(
            [
                self._chain_record(
                    wallet,
                    voter,
                    Asset.native().code,
                    "",
                    expected_amount,
                    "tx-midcross",
                    "Bribe: {}".format(market.short_value),
                    created_at.isoformat(),
                ),
            ],
            [],
        )

        report = self._reconcile(d_prev, d_chain, wallet, server=server)

        self.assertEqual(len(report.ambiguous), 0)
        chain_only_with_vs = [row for row in report.chain_only if row[6] is not None]
        self.assertEqual(len(chain_only_with_vs), 1)
        self.assertEqual(chain_only_with_vs[0][5], bribe.id)
        self.assertEqual(chain_only_with_vs[0][6], vote_prev.id)

    def test_process_page_connection_error_persists_retryable_timeout(self):
        # X3 (2026-04-24 audit): `stellar_sdk.exceptions.ConnectionError` is
        # a sibling of BaseHorizonError, not a subclass — prior to this
        # fix it fell through to the generic `except Exception` branch and
        # wrote `message=str(exc)`, which is neither re-checked by
        # _clean_failed_payouts nor in the _clean_rewards retryable set, so
        # the voter was silently dropped from every future run.
        from stellar_sdk.exceptions import ConnectionError as StellarConnectionError

        snapshot_date = timezone.now().date()
        market = self._make_market()
        bribe = self._make_bribe(
            market, asset_code=Asset.native().code, asset_issuer=""
        )
        self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "100000"
        )
        mock_envelope = mock.MagicMock()
        mock_envelope.sign = mock.MagicMock()
        mock_envelope.hash_hex = mock.MagicMock(return_value="envhash")
        mock_server = mock.MagicMock()
        mock_server.submit_transaction = mock.MagicMock(
            side_effect=StellarConnectionError("connection reset")
        )
        votes = VoteSnapshot.objects.filter(
            market_key=market, snapshot_time=snapshot_date
        )
        total_votes = votes.aggregate(total=models.Sum("votes_value"))["total"]

        wallet = SecuredWallet(public_key="G" + "A" * 55, secret="S" + "A" * 55)
        payer = RewardPayer(bribe, wallet, bribe.asset, Decimal("100"), stop_at=None)
        payer.server = mock_server
        with mock.patch.object(payer, "_build_transaction", return_value=mock_envelope):
            payer.pay_reward(votes, total_votes=total_votes)

        payouts = list(Payout.objects.filter(bribe=bribe))
        self.assertEqual(len(payouts), 1)
        self.assertEqual(payouts[0].status, Payout.STATUS_FAILED)
        self.assertEqual(payouts[0].message, "timeout")
        self.assertEqual(payouts[0].stellar_transaction_id, "envhash")

    def test_process_page_horizon_error_empty_extras_is_retryable(self):
        # X3 (2026-04-24 audit): BaseHorizonError with extras=None (Horizon
        # returned a non-JSON body — typically a 500 HTML page) used to
        # persist `message='no_reason'`, which is neither re-checked nor in
        # the retryable set. Must become `unknown_response_no_successful_field`
        # so the next run re-verifies the hash on Horizon.
        snapshot_date = timezone.now().date()
        market = self._make_market()
        bribe = self._make_bribe(
            market, asset_code=Asset.native().code, asset_issuer=""
        )
        self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "100000"
        )
        mock_envelope = mock.MagicMock()
        mock_envelope.sign = mock.MagicMock()
        mock_envelope.hash_hex = mock.MagicMock(return_value="hexhex")
        exc = BaseHorizonError.__new__(BaseHorizonError)
        exc.extras = None
        exc.status = 500
        mock_server = mock.MagicMock()
        mock_server.submit_transaction = mock.MagicMock(side_effect=exc)
        votes = VoteSnapshot.objects.filter(
            market_key=market, snapshot_time=snapshot_date
        )
        total_votes = votes.aggregate(total=models.Sum("votes_value"))["total"]

        wallet = SecuredWallet(public_key="G" + "A" * 55, secret="S" + "A" * 55)
        payer = RewardPayer(bribe, wallet, bribe.asset, Decimal("100"), stop_at=None)
        payer.server = mock_server
        with mock.patch.object(payer, "_build_transaction", return_value=mock_envelope):
            payer.pay_reward(votes, total_votes=total_votes)

        payouts = list(Payout.objects.filter(bribe=bribe))
        self.assertEqual(len(payouts), 1)
        self.assertEqual(payouts[0].status, Payout.STATUS_FAILED)
        # X4 fix: `timeout` (not `unknown_response_no_successful_field`) so
        # `_clean_failed_payouts` keeps the 5-min grace before re-checking.
        # Without the grace, a transient NotFoundError on the very next run
        # would `.delete()` this row and let a fresh submit double-pay once
        # the original tx propagates.
        self.assertEqual(payouts[0].message, "timeout")
        self.assertEqual(payouts[0].stellar_transaction_id, "hexhex")

    def test_process_page_unexpected_exception_persists_recheckable(self):
        # X3 (2026-04-24 audit): anything reaching the generic `except
        # Exception` branch after envelope signing must persist a message
        # that triggers Horizon re-verification — otherwise a single
        # unexpected error type removes the voter from all future payouts.
        import aquarius_bribes.rewards.reward_payer as rp_module

        snapshot_date = timezone.now().date()
        market = self._make_market()
        bribe = self._make_bribe(
            market, asset_code=Asset.native().code, asset_issuer=""
        )
        self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "100000"
        )
        mock_envelope = mock.MagicMock()
        mock_envelope.sign = mock.MagicMock()
        mock_envelope.hash_hex = mock.MagicMock(return_value="unexp-hash")
        mock_server = mock.MagicMock()
        mock_server.submit_transaction = mock.MagicMock(
            side_effect=RuntimeError("surprise")
        )
        votes = VoteSnapshot.objects.filter(
            market_key=market, snapshot_time=snapshot_date
        )
        total_votes = votes.aggregate(total=models.Sum("votes_value"))["total"]

        wallet = SecuredWallet(public_key="G" + "A" * 55, secret="S" + "A" * 55)
        payer = RewardPayer(bribe, wallet, bribe.asset, Decimal("100"), stop_at=None)
        payer.server = mock_server
        with mock.patch.object(rp_module, "sentry_sdk") as mock_sentry:
            with mock.patch.object(
                payer, "_build_transaction", return_value=mock_envelope
            ):
                payer.pay_reward(votes, total_votes=total_votes)

        mock_sentry.capture_exception.assert_called()
        payouts = list(Payout.objects.filter(bribe=bribe))
        self.assertEqual(len(payouts), 1)
        self.assertEqual(payouts[0].status, Payout.STATUS_FAILED)
        # X4 fix: `timeout` not `unknown_response_no_successful_field` — the
        # generic Exception branch has no guarantee the tx reached Horizon,
        # so the 5-min grace is required before `_clean_failed_payouts`
        # re-checks (otherwise transient NotFoundError deletes the row and
        # enables a double-pay when the original tx propagates late).
        self.assertEqual(payouts[0].message, "timeout")
        self.assertEqual(payouts[0].stellar_transaction_id, "unexp-hash")

    def test_horizon_error_empty_extras_fresh_row_survives_hourly_recheck(self):
        # X4 (2026-04-24 audit): a Payout persisted via the BaseHorizonError
        # empty-extras path must NOT be deleted by `_clean_failed_payouts`
        # on a re-run inside the 5-min grace window, even if Horizon returns
        # NotFoundError for its hash. Without the grace the row would be
        # deleted, `_clean_rewards` would re-enqueue the voter, and a fresh
        # submit could double-pay once the original tx propagates.
        from stellar_sdk.exceptions import NotFoundError

        snapshot_date = timezone.now().date()
        market = self._make_market()
        bribe = self._make_bribe(
            market, asset_code=Asset.native().code, asset_issuer=""
        )
        self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "100000"
        )
        mock_envelope = mock.MagicMock()
        mock_envelope.sign = mock.MagicMock()
        mock_envelope.hash_hex = mock.MagicMock(return_value="pending-hash")
        exc = BaseHorizonError.__new__(BaseHorizonError)
        exc.extras = None
        exc.status = 500
        mock_server = mock.MagicMock()
        mock_server.submit_transaction = mock.MagicMock(side_effect=exc)
        # Horizon would 404 the hash on immediate re-check (tx hasn't
        # propagated yet). The 5-min grace must prevent the re-check from
        # firing.
        tx_call = mock.MagicMock()
        tx_call.call = mock.MagicMock(side_effect=NotFoundError(mock.MagicMock()))
        transactions_builder = mock.MagicMock()
        transactions_builder.transaction = mock.MagicMock(return_value=tx_call)
        mock_server.transactions = mock.MagicMock(return_value=transactions_builder)
        votes = VoteSnapshot.objects.filter(
            market_key=market, snapshot_time=snapshot_date
        )
        total_votes = votes.aggregate(total=models.Sum("votes_value"))["total"]

        wallet = SecuredWallet(public_key="G" + "A" * 55, secret="S" + "A" * 55)
        payer = RewardPayer(bribe, wallet, bribe.asset, Decimal("100"), stop_at=None)
        payer.server = mock_server
        with mock.patch.object(payer, "_build_transaction", return_value=mock_envelope):
            payer.pay_reward(votes, total_votes=total_votes)

        # Fresh run persisted one FAILED Payout with message='timeout'.
        self.assertEqual(Payout.objects.filter(bribe=bribe).count(), 1)
        persisted = Payout.objects.get(bribe=bribe)
        self.assertEqual(persisted.message, "timeout")

        # Operator re-runs pay_reward ~1 min later (before the 5-min grace).
        votes = VoteSnapshot.objects.filter(
            market_key=market, snapshot_time=snapshot_date
        )
        payer2 = RewardPayer(bribe, wallet, bribe.asset, Decimal("100"), stop_at=None)
        payer2.server = mock_server
        payer2.pay_reward(votes, total_votes=total_votes)

        # Row must still exist: 5-min grace kept `_clean_failed_payouts`
        # away from the hash; _clean_rewards excluded the voter because
        # 'timeout' is NOT in the retryable set. No second submit → no
        # double-pay.
        self.assertEqual(Payout.objects.filter(bribe=bribe).count(), 1)
        self.assertTrue(Payout.objects.filter(pk=persisted.pk).exists())

    def test_unexpected_exception_fresh_row_survives_hourly_recheck(self):
        # X4 (2026-04-24 audit): same guarantee as above for the generic
        # `except Exception` branch — persisted message='timeout' must not
        # be deleted by a <5-min re-run that sees NotFoundError.
        import aquarius_bribes.rewards.reward_payer as rp_module
        from stellar_sdk.exceptions import NotFoundError

        snapshot_date = timezone.now().date()
        market = self._make_market()
        bribe = self._make_bribe(
            market, asset_code=Asset.native().code, asset_issuer=""
        )
        self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "100000"
        )
        mock_envelope = mock.MagicMock()
        mock_envelope.sign = mock.MagicMock()
        mock_envelope.hash_hex = mock.MagicMock(return_value="surprise-hash")
        mock_server = mock.MagicMock()
        mock_server.submit_transaction = mock.MagicMock(
            side_effect=RuntimeError("surprise")
        )
        tx_call = mock.MagicMock()
        tx_call.call = mock.MagicMock(side_effect=NotFoundError(mock.MagicMock()))
        transactions_builder = mock.MagicMock()
        transactions_builder.transaction = mock.MagicMock(return_value=tx_call)
        mock_server.transactions = mock.MagicMock(return_value=transactions_builder)
        votes = VoteSnapshot.objects.filter(
            market_key=market, snapshot_time=snapshot_date
        )
        total_votes = votes.aggregate(total=models.Sum("votes_value"))["total"]

        wallet = SecuredWallet(public_key="G" + "A" * 55, secret="S" + "A" * 55)
        payer = RewardPayer(bribe, wallet, bribe.asset, Decimal("100"), stop_at=None)
        payer.server = mock_server
        with mock.patch.object(rp_module, "sentry_sdk"):
            with mock.patch.object(payer, "_build_transaction", return_value=mock_envelope):
                payer.pay_reward(votes, total_votes=total_votes)

        persisted = Payout.objects.get(bribe=bribe)
        self.assertEqual(persisted.message, "timeout")

        payer2 = RewardPayer(bribe, wallet, bribe.asset, Decimal("100"), stop_at=None)
        payer2.server = mock_server
        votes = VoteSnapshot.objects.filter(
            market_key=market, snapshot_time=snapshot_date
        )
        with mock.patch.object(rp_module, "sentry_sdk"):
            payer2.pay_reward(votes, total_votes=total_votes)

        self.assertEqual(Payout.objects.filter(bribe=bribe).count(), 1)
        self.assertTrue(Payout.objects.filter(pk=persisted.pk).exists())

    def test_chain_only_weekly_rollover_midnight_cross_returns_ambiguous(self):
        # X5 (2026-04-24 audit): two consecutive weekly bribes on the same
        # market_key with bribe_A stopping exactly at D 00:00 UTC and bribe_B
        # starting at D 00:00 UTC. A tx submitted for snapshot_time=D-1 that
        # lands on-chain at D 00:05 must NOT be silently attributed to
        # bribe_B / VS_D when the voter also has VS_D. Route to AMBIGUOUS so
        # operator can disambiguate (safer than pre-fix behaviour in terms
        # of recovery, since pre-fix also returned AMBIGUOUS here).
        wallet = Keypair.random().public_key
        today = timezone.now().date()
        d_prev = today - timedelta(days=3)
        d_chain = today - timedelta(days=2)
        market = self._make_market()
        bribe_a = AggregatedByAssetBribe.objects.create(
            market_key=market,
            asset_code=Asset.native().code,
            asset_issuer="",
            start_at=self._at(d_prev, 0),
            stop_at=self._at(d_chain, 0),  # stops exactly at d_chain 00:00 UTC
            total_reward_amount=Decimal("700"),
        )
        AggregatedByAssetBribe.objects.create(
            market_key=market,
            asset_code=Asset.native().code,
            asset_issuer="",
            start_at=self._at(d_chain, 0),  # starts exactly at d_chain 00:00 UTC
            stop_at=self._at(d_chain, 0) + timedelta(days=7),
            total_reward_amount=Decimal("700"),
        )
        voter = Keypair.random().public_key
        self._make_vote(market, voter, d_prev, "100")
        self._make_vote(market, voter, d_chain, "100")

        # Chain op lands at d_chain 00:05 UTC — a midnight-cross of
        # bribe_a's last submission.
        created_at = self._at(d_chain, 0) + timedelta(minutes=5)
        server, _ = self._mock_reconcile_server(
            [
                self._chain_record(
                    wallet,
                    voter,
                    Asset.native().code,
                    "",
                    Decimal(bribe_a.daily_amount).quantize(
                        Decimal("0.0000001"), rounding=ROUND_DOWN,
                    ),
                    "tx-rollover",
                    "Bribe: {}".format(market.short_value),
                    created_at.isoformat(),
                ),
            ],
            [],
        )

        report = self._reconcile(d_prev, d_chain, wallet, server=server)

        # Must land in ambiguous, not silently attributed to bribe_b/VS_{d_chain}.
        self.assertEqual(len(report.ambiguous), 1)
        self.assertEqual(len(report.chain_only), 0)

    def test_clean_failed_payouts_skips_already_success_timeout_row(self):
        # X4 iter-3 (2026-04-24 audit): a previous run upgraded a FAILED +
        # message='timeout' row to SUCCESS but left the message intact. A
        # later run must NOT re-check the hash (and therefore cannot be
        # tricked into deleting the SUCCESS row on a transient NotFoundError
        # response). The STATUS_FAILED filter guard in _clean_failed_payouts
        # is load-bearing for that guarantee.
        from stellar_sdk.exceptions import NotFoundError

        market = self._make_market()
        bribe = self._make_bribe(
            market, asset_code=Asset.native().code, asset_issuer=""
        )
        snapshot_date = timezone.now().date()
        vote = self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "100"
        )
        payout = Payout.objects.create(
            bribe=bribe,
            vote_snapshot=vote,
            asset_code=bribe.asset_code,
            asset_issuer=bribe.asset_issuer or "",
            reward_amount=Decimal("1.0000000"),
            stellar_transaction_id="landed-hash",
            status=Payout.STATUS_SUCCESS,
            message="timeout",  # legacy shape from a pre-guard upgrade
        )
        Payout.objects.filter(pk=payout.pk).update(
            created_at=timezone.now() - timedelta(minutes=15),
        )
        # Horizon would 404 this hash on re-fetch (transient node-switch or
        # indexing gap). If the re-check ran it would delete the SUCCESS
        # row, re-enqueue the voter, and enable a double-pay.
        tx_call = mock.MagicMock()
        tx_call.call = mock.MagicMock(side_effect=NotFoundError(mock.MagicMock()))
        transactions_builder = mock.MagicMock()
        transactions_builder.transaction = mock.MagicMock(return_value=tx_call)
        mock_server = mock.MagicMock()
        mock_server.transactions = mock.MagicMock(return_value=transactions_builder)

        wallet = SecuredWallet(public_key="G" + "A" * 55, secret="S" + "A" * 55)
        payer = RewardPayer(bribe, wallet, bribe.asset, bribe.total_reward_amount)
        payer.server = mock_server

        payer._clean_failed_payouts(
            VoteSnapshot.objects.filter(market_key=market, snapshot_time=snapshot_date)
        )

        # Row must still exist as SUCCESS — the STATUS_FAILED guard kept
        # `uncertain_transactions` from picking it up, and the .delete() /
        # .update() calls also filter by status so they never touch it.
        transactions_builder.transaction.assert_not_called()
        self.assertTrue(Payout.objects.filter(pk=payout.pk).exists())
        payout.refresh_from_db()
        self.assertEqual(payout.status, Payout.STATUS_SUCCESS)

    def test_clean_failed_payouts_upgrade_stamps_message_reverified(self):
        # X4 iter-3 (2026-04-24 audit): on successful re-verification, the
        # FAILED-with-timeout row is upgraded to SUCCESS with
        # message='reverified_after_timeout'. Future runs then see a
        # non-`timeout`/non-`unknown_response_*` message and would not
        # re-check the hash even if someone later loosens the status guard.
        market = self._make_market()
        bribe = self._make_bribe(
            market, asset_code=Asset.native().code, asset_issuer=""
        )
        snapshot_date = timezone.now().date()
        vote = self._make_vote(
            market, Keypair.random().public_key, snapshot_date, "100"
        )
        payout = Payout.objects.create(
            bribe=bribe,
            vote_snapshot=vote,
            asset_code=bribe.asset_code,
            asset_issuer=bribe.asset_issuer or "",
            reward_amount=Decimal("1.0000000"),
            stellar_transaction_id="landed-hash-2",
            status=Payout.STATUS_FAILED,
            message="timeout",
        )
        Payout.objects.filter(pk=payout.pk).update(
            created_at=timezone.now() - timedelta(minutes=15),
        )
        tx_call = mock.MagicMock()
        tx_call.call = mock.MagicMock(return_value={"successful": True})
        transactions_builder = mock.MagicMock()
        transactions_builder.transaction = mock.MagicMock(return_value=tx_call)
        mock_server = mock.MagicMock()
        mock_server.transactions = mock.MagicMock(return_value=transactions_builder)

        wallet = SecuredWallet(public_key="G" + "A" * 55, secret="S" + "A" * 55)
        payer = RewardPayer(bribe, wallet, bribe.asset, bribe.total_reward_amount)
        payer.server = mock_server

        payer._clean_failed_payouts(
            VoteSnapshot.objects.filter(market_key=market, snapshot_time=snapshot_date)
        )

        payout.refresh_from_db()
        self.assertEqual(payout.status, Payout.STATUS_SUCCESS)
        self.assertEqual(payout.message, "reverified_after_timeout")

    def test_chain_only_same_bribe_two_dates_resolves_via_amount(self):
        # X5 iter-3 (2026-04-24 audit): a recurring voter on a single multi-
        # day bribe has VSs on both D-1 and D. Per-date candidate resolution
        # must do amount-match across dates and return the concrete
        # (bribe, VS) when amount uniquely identifies one, _AMBIGUOUS
        # otherwise. Same-day preference alone would silently misattribute
        # a midnight-cross tx for VS_{D-1} landing on D to VS_D.
        wallet = Keypair.random().public_key
        today = timezone.now().date()
        d_prev = today - timedelta(days=3)
        d_chain = today - timedelta(days=2)
        market = self._make_market()
        bribe = self._make_bribe(
            market,
            asset_code=Asset.native().code,
            asset_issuer="",
            start=self._at(d_prev, 0),
            stop=self._at(d_chain, 0) + timedelta(days=1),
        )
        voter = Keypair.random().public_key
        vote_prev = self._make_vote(market, voter, d_prev, "100")
        # Total on D-1: voter=100, other=100 → 200 → expected for voter
        # = daily * 100/200 = daily/2. On D voter is alone so total=100
        # → expected = daily. Chain op amount = daily/2 uniquely matches
        # VS_{d_prev} (midnight-cross shape).
        self._make_vote(market, Keypair.random().public_key, d_prev, "100")
        self._make_vote(market, voter, d_chain, "100")

        created_at = self._at(d_chain, 0) + timedelta(minutes=5)
        expected_amount = (
            Decimal(bribe.daily_amount)
            * Decimal(vote_prev.votes_value)
            / Decimal("200")
        ).quantize(Decimal("0.0000001"), rounding=ROUND_DOWN)
        server, _ = self._mock_reconcile_server(
            [
                self._chain_record(
                    wallet,
                    voter,
                    Asset.native().code,
                    "",
                    expected_amount,
                    "tx-amount-match",
                    "Bribe: {}".format(market.short_value),
                    created_at.isoformat(),
                ),
            ],
            [],
        )

        report = self._reconcile(d_prev, d_chain, wallet, server=server)

        self.assertEqual(len(report.ambiguous), 0)
        chain_only_with_vs = [row for row in report.chain_only if row[6] is not None]
        self.assertEqual(len(chain_only_with_vs), 1)
        # Must resolve to VS_{d_prev}, not VS_{d_chain}, via amount-match.
        self.assertEqual(chain_only_with_vs[0][5], bribe.id)
        self.assertEqual(chain_only_with_vs[0][6], vote_prev.id)
