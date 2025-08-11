from datetime import timedelta
from decimal import Decimal
from unittest import mock

from django.conf import settings
from django.db import models
from django.test import TestCase, override_settings
from django.utils import timezone

import requests
from constance import config
from stellar_sdk import Asset, Claimant, ClaimPredicate, Keypair, Server, TransactionBuilder

from aquarius_bribes.bribes.models import AggregatedByAssetBribe, Bribe, MarketKey
from aquarius_bribes.bribes.tasks import task_aggregate_bribes, load_market_key_details
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
