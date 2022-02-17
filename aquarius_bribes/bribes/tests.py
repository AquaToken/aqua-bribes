from django.conf import settings
from django.test import TestCase, override_settings
from django.utils import timezone

import requests

from constance import config
from datetime import timedelta
from decimal import Decimal
from stellar_sdk import Asset, Claimant, ClaimPredicate
from stellar_sdk import Keypair, Server, TransactionBuilder

from aquarius_bribes.bribes.loader import BribesLoader
from aquarius_bribes.bribes.models import Bribe
from aquarius_bribes.bribes.tasks import claim_bribes, return_bribes


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

        print(self.bribe_wallet.public_key, self.bribe_wallet.secret)
        print(self.account_1.public_key, self.account_1.secret)
        print(self.default_market_key.public_key, self.default_market_key.secret)
        print(self.asset_xxx_issuer.public_key, self.asset_xxx_issuer.secret)
        print(random_asset_issuer.public_key, random_asset_issuer.secret)

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

    def _test_correct_bribe_parsing(self):
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

    def _test_bribe_claim_without_path(self):
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

        claim_bribes()

        self.assertEqual(Bribe.objects.first().status, Bribe.STATUS_NO_PATH_FOR_CONVERSION)


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

        return
        claim_bribes()

        self.assertEqual(Bribe.objects.first().status, Bribe.STATUS_ACTIVE)
        self.assertEqual(Bribe.objects.first().amount_for_bribes, Decimal('96.9696969'))
        self.assertEqual(Bribe.objects.first().amount_aqua, config.CONVERTATION_AMOUNT)

    def _test_bribe_claim_without_path_and_return(self):
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

        claim_bribes()

        self.assertEqual(Bribe.objects.first().status, Bribe.STATUS_NO_PATH_FOR_CONVERSION)

        return_bribes()

        self.assertEqual(Bribe.objects.first().status, Bribe.STATUS_RETURNED)

        claims = self.server.claimable_balances().for_claimant(
            self.bribe_wallet.public_key,
        ).limit(100).order(
            desc=False,
        ).call()['_embedded']['records']

        self.assertEqual(len(claims), 0)
