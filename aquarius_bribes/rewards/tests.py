from django.conf import settings
from django.test import TestCase, override_settings
from django.utils import timezone

import requests

from constance import config
from datetime import timedelta
from decimal import Decimal
from stellar_sdk import Asset, Claimant, ClaimPredicate
from stellar_sdk import Keypair, Server, TransactionBuilder

from aquarius_bribes.bribes.models import Bribe
from aquarius_bribes.rewards.models import Payout, VoteSnapshot
from aquarius_bribes.rewards.reward_payer import RewardPayer
from aquarius_bribes.rewards.votes_loader import VotesLoader
from aquarius_bribes.rewards.utils import SecuredWallet


random_asset_issuer = Keypair.random()
bribe_wallet = Keypair.random()


@override_settings(
    REWARD_ASSET_CODE='ZZZ', REWARD_ASSET_ISSUER=random_asset_issuer.public_key,
    BRIBE_WALLET_ADDRESS=bribe_wallet.public_key, BRIBE_WALLET_SIGNER=bribe_wallet.secret,
)
class BribesTests(TestCase):
    def _create_account(self, account):
        # Fund new wallet
        response = requests.get('https://friendbot.stellar.org/?addr={}'.format(account))

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
            asset_code=asset.code, asset_issuer=asset.issuer, source=account.public_key,
        )

        return builder

    def _payment(self, source, destination, asset, amount, builder=None):
        if builder is None:
            builder = self._get_builder(source)

        builder.append_payment_op(
            destination=destination.public_key,
            asset_code=asset.code,
            asset_issuer=asset.issuer,
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
        except:
            self._create_account(account)
            account_info = self._load_or_create_account(account)

        return account_info

    def setUp(self):
        self.server = Server(settings.HORIZON_URL)
        self.bribe_wallet = bribe_wallet
        self.account_1 = Keypair.random()
        self.default_market_key = Keypair.random()
        self.asset_xxx_issuer = Keypair.random()
        self.reward_asset = Asset(code=settings.REWARD_ASSET_CODE, issuer=settings.REWARD_ASSET_ISSUER)

        self.asset_xxx = Asset(code='XXX', issuer=self.asset_xxx_issuer.public_key)
        self._load_or_create_account(self.asset_xxx_issuer.public_key)
        self._load_or_create_account(random_asset_issuer.public_key)

        self._load_or_create_account(self.bribe_wallet.public_key)
        self._load_or_create_account(self.account_1.public_key)

        builder = self._get_builder(self.asset_xxx_issuer)
        builder = self._trust_asset(self.account_1, self.asset_xxx, builder=builder)
        builder = self._trust_asset(self.bribe_wallet, self.reward_asset, builder=builder)
        builder = self._trust_asset(self.bribe_wallet, self.asset_xxx, builder=builder)
        builder = self._trust_asset(random_asset_issuer, self.asset_xxx, builder=builder)
        builder = self._payment(self.asset_xxx_issuer, self.account_1, self.asset_xxx, amount=1000, builder=builder)
        builder = self._payment(self.asset_xxx_issuer, self.bribe_wallet, self.asset_xxx, amount=100000000, builder=builder)
        builder = self._payment(random_asset_issuer, self.bribe_wallet, self.reward_asset, amount=100000000, builder=builder)
        transaction_envelope = builder.build()
        transaction_envelope.sign(self.asset_xxx_issuer.secret)
        transaction_envelope.sign(self.account_1.secret)
        transaction_envelope.sign(self.bribe_wallet.secret)
        transaction_envelope.sign(random_asset_issuer.secret)

        response = self.server.submit_transaction(transaction_envelope)

    def test_votes_loader(self):
        market_key = 'GBPF7NLFCYGZNHU6HS64ZGTE4YCRLAWTLFGOMFTHQ3WSUUFIGOSQFPJT'
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
        market_key = 'GBPF7NLFCYGZNHU6HS64ZGTE4YCRLAWTLFGOMFTHQ3WSUUFIGOSQFPJT'
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
            builder = self._payment(self.asset_xxx_issuer, voting_account, self.asset_xxx, amount=1000, builder=builder)

        transaction_envelope = builder.build()

        for i in range(10):
            transaction_envelope.sign(accounts[i].secret)

        transaction_envelope.sign(self.account_1.secret)
        transaction_envelope.sign(self.asset_xxx_issuer.secret)

        response = self.server.submit_transaction(transaction_envelope)
        VoteSnapshot.objects.bulk_create(votes)

        bribe = Bribe(
            amount_for_bribes=100000,
            amount_aqua=config.CONVERTATION_AMOUNT,
            asset_code=self.asset_xxx.code,
            asset_issuer=self.asset_xxx.issuer,
            status=Bribe.STATUS_ACTIVE,
            amount=100000,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        bribe.save()

        reward_wallet = SecuredWallet(
            public_key=settings.BRIBE_WALLET_ADDRESS,
            secret=settings.BRIBE_WALLET_SIGNER,
        )

        reward_period = timedelta(hours=1)
        reward_amount = bribe.daily_bribe_amount * Decimal(reward_period.total_seconds() / (24 * 3600))
        reward_payer = RewardPayer(bribe, reward_wallet, self.reward_asset, reward_amount)
        reward_payer.pay_reward(VoteSnapshot.objects.all())

        Payout.objects.values_list('status', flat=True).distinct()
        self.assertEqual(Payout.objects.values_list('status', flat=True).distinct().count(), 1)
        self.assertEqual(Payout.objects.values_list('status', flat=True).distinct().first(), Payout.STATUS_SUCCESS)

    def test_reward_payer_with_native_asset(self):
        market_key = 'GBPF7NLFCYGZNHU6HS64ZGTE4YCRLAWTLFGOMFTHQ3WSUUFIGOSQFPJT'
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
            builder = self._payment(self.asset_xxx_issuer, voting_account, self.asset_xxx, amount=1000, builder=builder)

        transaction_envelope = builder.build()

        for i in range(10):
            transaction_envelope.sign(accounts[i].secret)

        transaction_envelope.sign(self.account_1.secret)
        transaction_envelope.sign(self.asset_xxx_issuer.secret)

        response = self.server.submit_transaction(transaction_envelope)
        VoteSnapshot.objects.bulk_create(votes)

        bribe = Bribe(
            amount_for_bribes=100000,
            amount_aqua=config.CONVERTATION_AMOUNT,
            asset_code=Asset.native().code,
            asset_issuer=Asset.native().issuer or '',
            status=Bribe.STATUS_ACTIVE,
            amount=100000,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )
        bribe.save()

        reward_wallet = SecuredWallet(
            public_key=settings.BRIBE_WALLET_ADDRESS,
            secret=settings.BRIBE_WALLET_SIGNER,
        )

        reward_period = timedelta(hours=1)
        reward_amount = bribe.daily_bribe_amount * Decimal(reward_period.total_seconds() / (24 * 3600))
        reward_payer = RewardPayer(bribe, reward_wallet, self.reward_asset, reward_amount)
        reward_payer.pay_reward(VoteSnapshot.objects.all())

        Payout.objects.values_list('stellar_transaction_id', flat=True).distinct()
        self.assertEqual(Payout.objects.values_list('status', flat=True).distinct().count(), 1)
        self.assertEqual(Payout.objects.values_list('status', flat=True).distinct().first(), Payout.STATUS_SUCCESS)
