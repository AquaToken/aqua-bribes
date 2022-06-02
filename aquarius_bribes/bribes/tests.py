from django.conf import settings
from django.test import TestCase, override_settings
from django.utils import timezone

import requests
from unittest.mock import MagicMock, patch

from constance import config
from datetime import timedelta
from decimal import Decimal
from stellar_sdk import Account, Asset, Claimant, ClaimPredicate
from stellar_sdk import Keypair, Server, TransactionBuilder

from aquarius_bribes.bribes.loader import BribesLoader
from aquarius_bribes.bribes.models import AggregatedByAssetBribe, Bribe
from aquarius_bribes.bribes.tasks import task_aggregate_bribes, task_claim_bribes, task_return_bribes


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

        selling = Asset(code=settings.REWARD_ASSET_CODE, issuer=settings.REWARD_ASSET_ISSUER)
        builder.append_manage_buy_offer_op(
            selling=selling,
            buying=self.asset_xxx,
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
        builder = self._trust_asset(random_asset_issuer, self.asset_xxx, builder=builder)
        builder = self._payment(self.asset_xxx_issuer, self.account_1, self.asset_xxx, amount=1000, builder=builder)
        transaction_envelope = builder.build()
        transaction_envelope.sign(self.asset_xxx_issuer.secret)
        transaction_envelope.sign(self.account_1.secret)
        transaction_envelope.sign(self.bribe_wallet.secret)
        transaction_envelope.sign(random_asset_issuer.secret)

        response = self.server.submit_transaction(transaction_envelope)

    def test_bribe_claim_without_path(self):
        loader = BribesLoader(self.bribe_wallet.public_key, self.bribe_wallet.secret)
        loader.load_bribes()

        builder = self._get_builder(self.account_1)

        claim_after = timezone.now() + timedelta(seconds=1)
        claim_after_timestamp = int(claim_after.strftime("%s"))
        claimants = [
            Claimant(
                destination=self.bribe_wallet.public_key,
                predicate=ClaimPredicate.predicate_not(
                    ClaimPredicate.predicate_before_absolute_time(claim_after_timestamp)
                ),
            ),
            Claimant(
                destination=self.default_market_key.public_key,
                predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
            )
        ]
        builder = self._send_claim(self.account_1, claimants, self.asset_xxx, amount=100, builder=builder)
        transaction_envelope = builder.build()
        transaction_envelope.sign(self.account_1.secret)
        response = self.server.submit_transaction(transaction_envelope)

        loader = BribesLoader(self.bribe_wallet.public_key, self.bribe_wallet.secret)
        loader.load_bribes()

        self.assertEqual(Bribe.objects.count(), 1)
        self.assertEqual(Bribe.objects.first().status, Bribe.STATUS_PENDING)
        start_at = timezone.now()
        start_at = claim_after + timedelta(days=8 - claim_after.isoweekday())
        start_at = start_at.replace(hour=0, minute=0, second=0, microsecond=0)
        self.assertEqual(Bribe.objects.first().start_at, start_at)

        task_claim_bribes()

        self.assertEqual(Bribe.objects.first().status, Bribe.STATUS_NO_PATH_FOR_CONVERSION)


    def test_bribe_claim_without_path_and_return(self):
        loader = BribesLoader(self.bribe_wallet.public_key, self.bribe_wallet.secret)
        loader.load_bribes()

        builder = self._get_builder(self.account_1)

        claim_after = timezone.now() + timedelta(seconds=1)
        claim_after_timestamp = int(claim_after.strftime("%s"))
        claimants = [
            Claimant(
                destination=self.bribe_wallet.public_key,
                predicate=ClaimPredicate.predicate_not(
                    ClaimPredicate.predicate_before_absolute_time(claim_after_timestamp)
                ),
            ),
            Claimant(
                destination=self.default_market_key.public_key,
                predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
            )
        ]
        builder = self._send_claim(self.account_1, claimants, self.asset_xxx, amount=100, builder=builder)
        transaction_envelope = builder.build()
        transaction_envelope.sign(self.account_1.secret)
        response = self.server.submit_transaction(transaction_envelope)

        loader = BribesLoader(self.bribe_wallet.public_key, self.bribe_wallet.secret)
        loader.load_bribes()

        self.assertEqual(Bribe.objects.count(), 1)
        self.assertEqual(Bribe.objects.first().status, Bribe.STATUS_PENDING)
        start_at = timezone.now()
        start_at = claim_after + timedelta(days=8 - claim_after.isoweekday())
        start_at = start_at.replace(hour=0, minute=0, second=0, microsecond=0)
        self.assertEqual(Bribe.objects.first().start_at, start_at)

        task_claim_bribes()

        self.assertEqual(Bribe.objects.first().status, Bribe.STATUS_NO_PATH_FOR_CONVERSION)

        task_return_bribes()

        self.assertEqual(Bribe.objects.first().status, Bribe.STATUS_RETURNED)

        claims = self.server.claimable_balances().for_claimant(
            self.bribe_wallet.public_key,
        ).for_sponsor(self.account_1.public_key).limit(100).order(
            desc=False,
        ).call()['_embedded']['records']

        self.assertEqual(len(claims), 0)

    def test_correct_bribe_parsing(self):
        loader = BribesLoader(self.bribe_wallet.public_key, self.bribe_wallet.secret)
        loader.load_bribes()

        builder = self._get_builder(self.account_1)

        claim_after = timezone.now() + timedelta(days=7)
        # claim_after = timezone.now() + timedelta(minutes=1)
        claim_after_timestamp = int(claim_after.strftime("%s"))
        claimants = [
            Claimant(
                destination=self.bribe_wallet.public_key,
                predicate=ClaimPredicate.predicate_not(
                    ClaimPredicate.predicate_before_absolute_time(claim_after_timestamp)
                ),
            ),
            Claimant(
                destination=self.default_market_key.public_key,
                predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
            )
        ]
        builder = self._send_claim(self.account_1, claimants, self.asset_xxx, amount=100, builder=builder)
        transaction_envelope = builder.build()
        transaction_envelope.sign(self.account_1.secret)
        response = self.server.submit_transaction(transaction_envelope)

        loader = BribesLoader(self.bribe_wallet.public_key, self.bribe_wallet.secret)
        loader.load_bribes()

        self.assertEqual(Bribe.objects.count(), 1)
        self.assertEqual(Bribe.objects.first().status, Bribe.STATUS_PENDING)
        start_at = timezone.now()
        start_at = claim_after + timedelta(days=8 - claim_after.isoweekday())
        start_at = start_at.replace(hour=0, minute=0, second=0, microsecond=0)
        self.assertEqual(Bribe.objects.first().start_at, start_at)

    def test_bribe_claim_with_path(self):
        loader = BribesLoader(self.bribe_wallet.public_key, self.bribe_wallet.secret)
        loader.load_bribes()

        builder = self._get_builder(self.account_1)

        claim_after = timezone.now() + timedelta(microseconds=1)
        claim_after_timestamp = int(claim_after.strftime("%s"))
        claimants = [
            Claimant(
                destination=self.bribe_wallet.public_key,
                predicate=ClaimPredicate.predicate_not(
                    ClaimPredicate.predicate_before_absolute_time(claim_after_timestamp)
                ),
            ),
            Claimant(
                destination=self.default_market_key.public_key,
                predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
            )
        ]
        builder = self._send_claim(self.account_1, claimants, self.asset_xxx, amount=100, builder=builder)
        transaction_envelope = builder.build()
        transaction_envelope.sign(self.account_1.secret)
        response = self.server.submit_transaction(transaction_envelope)

        loader = BribesLoader(self.bribe_wallet.public_key, self.bribe_wallet.secret)
        loader.load_bribes()

        self.assertEqual(Bribe.objects.count(), 1)
        self.assertEqual(Bribe.objects.first().status, Bribe.STATUS_PENDING)
        start_at = timezone.now()
        start_at = claim_after + timedelta(days=8 - claim_after.isoweekday())
        start_at = start_at.replace(hour=0, minute=0, second=0, microsecond=0)
        self.assertEqual(Bribe.objects.first().start_at, start_at)

        config.CONVERTATION_AMOUNT = Decimal(1)
        self._prepare_orderbook(Decimal('100'), Decimal('0.33'))

        task_claim_bribes()

        self.assertEqual(Bribe.objects.first().status, Bribe.STATUS_ACTIVE)
        self.assertEqual(Bribe.objects.first().amount_for_bribes, Decimal('96.9696969'))
        self.assertEqual(Bribe.objects.first().amount_aqua, config.CONVERTATION_AMOUNT)

    def test_bribe_aggregation(self):
        loader = BribesLoader(self.bribe_wallet.public_key, self.bribe_wallet.secret)
        loader.load_bribes()

        builder = self._get_builder(self.account_1)

        claim_after = timezone.now() + timedelta(microseconds=1)
        claim_after_timestamp = int(claim_after.strftime("%s"))
        claimants = [
            Claimant(
                destination=self.bribe_wallet.public_key,
                predicate=ClaimPredicate.predicate_not(
                    ClaimPredicate.predicate_before_absolute_time(claim_after_timestamp)
                ),
            ),
            Claimant(
                destination=self.default_market_key.public_key,
                predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
            )
        ]
        builder = self._send_claim(self.account_1, claimants, self.asset_xxx, amount=100, builder=builder)
        builder = self._send_claim(self.account_1, claimants, self.asset_xxx, amount=100, builder=builder)
        transaction_envelope = builder.build()
        transaction_envelope.sign(self.account_1.secret)
        response = self.server.submit_transaction(transaction_envelope)

        loader = BribesLoader(self.bribe_wallet.public_key, self.bribe_wallet.secret)
        loader.load_bribes()

        self.assertEqual(Bribe.objects.count(), 2)
        self.assertEqual(list(Bribe.objects.values_list('status', flat=True).distinct()) , [Bribe.STATUS_PENDING, ])
        start_at = timezone.now()
        start_at = claim_after + timedelta(days=8 - claim_after.isoweekday())
        start_at = start_at.replace(hour=0, minute=0, second=0, microsecond=0)
        self.assertEqual(Bribe.objects.first().start_at, start_at)

        config.CONVERTATION_AMOUNT = Decimal(1)
        self._prepare_orderbook(Decimal('100'), Decimal('0.33'))

        task_claim_bribes()

        self.assertEqual(list(Bribe.objects.values_list('status', flat=True).distinct()), [Bribe.STATUS_ACTIVE, ])
        self.assertEqual(list(Bribe.objects.values_list('amount_for_bribes', flat=True).distinct()), [Decimal('96.9696969'), ])
        self.assertEqual(list(Bribe.objects.values_list('amount_aqua', flat=True).distinct()), [config.CONVERTATION_AMOUNT, ])

        task_aggregate_bribes()

        self.assertEqual(AggregatedByAssetBribe.objects.count(), 2)
        self.assertEqual(
            AggregatedByAssetBribe.objects.filter(
                asset_code=self.asset_xxx.code
            ).first().total_reward_amount, Decimal('96.9696969') * 2,
        )
        self.assertEqual(
            AggregatedByAssetBribe.objects.filter(
                asset_code=self.reward_asset.code
            ).first().total_reward_amount, config.CONVERTATION_AMOUNT * 2,
        )

    def test_bribe_with_reward_asset_claim_with_path(self):
        loader = BribesLoader(self.bribe_wallet.public_key, self.bribe_wallet.secret)
        loader.load_bribes()

        builder = self._get_builder(self.account_1)

        claim_after = timezone.now() + timedelta(microseconds=1)
        claim_after_timestamp = int(claim_after.strftime("%s"))
        claimants = [
            Claimant(
                destination=self.bribe_wallet.public_key,
                predicate=ClaimPredicate.predicate_not(
                    ClaimPredicate.predicate_before_absolute_time(claim_after_timestamp)
                ),
            ),
            Claimant(
                destination=self.default_market_key.public_key,
                predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
            )
        ]
        builder = self._trust_asset(self.account_1, self.reward_asset, builder=builder)
        builder = self._payment(random_asset_issuer, self.account_1, self.reward_asset, amount=10000, builder=builder)
        builder = self._send_claim(self.account_1, claimants, self.reward_asset, amount=100, builder=builder)
        transaction_envelope = builder.build()
        transaction_envelope.sign(self.account_1.secret)
        transaction_envelope.sign(random_asset_issuer.secret)
        response = self.server.submit_transaction(transaction_envelope)

        loader = BribesLoader(self.bribe_wallet.public_key, self.bribe_wallet.secret)
        loader.load_bribes()

        self.assertEqual(Bribe.objects.count(), 1)
        self.assertEqual(Bribe.objects.first().status, Bribe.STATUS_PENDING)
        start_at = timezone.now()
        start_at = claim_after + timedelta(days=8 - claim_after.isoweekday())
        start_at = start_at.replace(hour=0, minute=0, second=0, microsecond=0)
        self.assertEqual(Bribe.objects.first().start_at, start_at)

        config.CONVERTATION_AMOUNT = Decimal(1)
        self._prepare_orderbook(Decimal('100'), Decimal('0.33'))

        task_claim_bribes()

        self.assertEqual(Bribe.objects.first().status, Bribe.STATUS_ACTIVE)
        self.assertEqual(Bribe.objects.first().amount_for_bribes, Decimal('99'))
        self.assertEqual(Bribe.objects.first().amount_aqua, config.CONVERTATION_AMOUNT)

    def test_bribe_with_native_asset_claim_with_path(self):
        loader = BribesLoader(self.bribe_wallet.public_key, self.bribe_wallet.secret)
        loader.load_bribes()

        builder = self._get_builder(self.account_1)

        claim_after = timezone.now() + timedelta(microseconds=1)
        claim_after_timestamp = int(claim_after.strftime("%s"))
        claimants = [
            Claimant(
                destination=self.bribe_wallet.public_key,
                predicate=ClaimPredicate.predicate_not(
                    ClaimPredicate.predicate_before_absolute_time(claim_after_timestamp)
                ),
            ),
            Claimant(
                destination=self.default_market_key.public_key,
                predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
            )
        ]
        self.asset_xxx = Asset.native()
        builder = self._send_claim(self.account_1, claimants, self.asset_xxx, amount=100, builder=builder)
        transaction_envelope = builder.build()
        transaction_envelope.sign(self.account_1.secret)
        response = self.server.submit_transaction(transaction_envelope)

        loader = BribesLoader(self.bribe_wallet.public_key, self.bribe_wallet.secret)
        loader.load_bribes()

        self.assertEqual(Bribe.objects.count(), 1)
        self.assertEqual(Bribe.objects.first().status, Bribe.STATUS_PENDING)
        self.assertEqual(Bribe.objects.first().asset, self.asset_xxx)
        start_at = timezone.now()
        start_at = claim_after + timedelta(days=8 - claim_after.isoweekday())
        start_at = start_at.replace(hour=0, minute=0, second=0, microsecond=0)
        self.assertEqual(Bribe.objects.first().start_at, start_at)

        config.CONVERTATION_AMOUNT = Decimal(1)
        self._prepare_orderbook(Decimal('100'), Decimal('0.33'))

        task_claim_bribes()

        self.assertEqual(Bribe.objects.first().status, Bribe.STATUS_ACTIVE)
        self.assertEqual(Bribe.objects.first().amount_for_bribes, Decimal('96.9696969'))
        self.assertEqual(Bribe.objects.first().amount_aqua, config.CONVERTATION_AMOUNT)

    def _test_bribe_claim_bad_seq(self):
        loader = BribesLoader(self.bribe_wallet.public_key, self.bribe_wallet.secret)
        loader.load_bribes()

        builder = self._get_builder(self.account_1)

        claim_after = timezone.now() + timedelta(microseconds=1)
        claim_after_timestamp = int(claim_after.strftime("%s"))
        claimants = [
            Claimant(
                destination=self.bribe_wallet.public_key,
                predicate=ClaimPredicate.predicate_not(
                    ClaimPredicate.predicate_before_absolute_time(claim_after_timestamp)
                ),
            ),
            Claimant(
                destination=self.default_market_key.public_key,
                predicate=ClaimPredicate.predicate_not(ClaimPredicate.predicate_unconditional()),
            )
        ]
        builder = self._trust_asset(self.account_1, self.reward_asset, builder=builder)
        builder = self._payment(random_asset_issuer, self.account_1, self.reward_asset, amount=10000, builder=builder)
        builder = self._send_claim(self.account_1, claimants, self.reward_asset, amount=100, builder=builder)
        transaction_envelope = builder.build()
        transaction_envelope.sign(self.account_1.secret)
        transaction_envelope.sign(random_asset_issuer.secret)
        response = self.server.submit_transaction(transaction_envelope)

        loader = BribesLoader(self.bribe_wallet.public_key, self.bribe_wallet.secret)
        loader.load_bribes()

        self.assertEqual(Bribe.objects.count(), 1)
        self.assertEqual(Bribe.objects.first().status, Bribe.STATUS_PENDING)
        start_at = timezone.now()
        start_at = claim_after + timedelta(days=8 - claim_after.isoweekday())
        start_at = start_at.replace(hour=0, minute=0, second=0, microsecond=0)
        self.assertEqual(Bribe.objects.first().start_at, start_at)

        config.CONVERTATION_AMOUNT = Decimal(1)
        self._prepare_orderbook(Decimal('100'), Decimal('0.33'))

        with patch(
            'stellar_sdk.server.Server.load_account',
            new=MagicMock(return_value=Account(account=self.bribe_wallet.public_key, sequence=0))
        ):
            task_claim_bribes()

        self.assertEqual(Bribe.objects.first().status, Bribe.STATUS_PENDING)
