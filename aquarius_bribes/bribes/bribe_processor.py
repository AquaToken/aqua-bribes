from django.conf import settings

from constance import config
from stellar_sdk import Asset, TransactionBuilder

from aquarius_bribes.bribes.exceptions import NoPathForConversionError
from aquarius_bribes.bribes.utils import get_horizon


class BribeProcessor(object):
    def __init__(
        self, bribe_address, bribe_signer, convert_to_asset,
        network_passphrase=settings.STELLAR_PASSPHRASE,
        base_fee=settings.BASE_FEE,
    ):
        self.bribe_address = bribe_address
        self.bribe_signer = bribe_signer
        self.network_passphrase = network_passphrase
        self.base_fee = base_fee
        self.horizon = get_horizon()
        self.convert_to_asset = convert_to_asset

    def get_account_info(self, address):
        try:
            return self.horizon.accounts().account_id(address).call()
        except:
            return None

    def has_trustline(self, asset, address):
        account_info = self.get_account_info(address)

        for balance in account_info.get('balances', []):
            if asset.is_native() and balance['asset_type'] == 'native':
                return balance
            if asset.code == balance.get('asset_code') and asset.issuer == balance.get('asset_issuer'):
                return balance

        return False

    def _get_builder(self):
        return TransactionBuilder(
            source_account=self.horizon.load_account(self.bribe_address),
            network_passphrase=self.network_passphrase,
            base_fee=self.base_fee,
        )

    def _get_path(self, source_asset, dest_asset, amount):
        paths = self.horizon.strict_receive_paths(
            source=[source_asset, ], destination_asset=dest_asset, destination_amount=amount,
        ).call().get('_embedded', {}).get('records', [])

        if len(paths) == 0:
            raise NoPathForConversionError()

        path = [source_asset]
        for item in paths[0]['path']:
            if item['asset_type'] == Asset.native().type:
                path.append(Asset.native())
            else:
                path.append(Asset(code=item['asset_code'], issuer=item['asset_issuer']))

        path.append(dest_asset)

        return path

    def convert_asset(self, bribe, using_builder=None):
        builder = using_builder or self._get_builder()

        path = self._get_path(bribe.asset, self.convert_to_asset, config.CONVERTATION_AMOUNT)

        builder.append_path_payment_strict_receive_op(
            destination=self.bribe_address,
            send_code=bribe.asset_code,
            send_issuer=bribe.asset_issuer,
            send_max=bribe.amount,
            dest_code=self.convert_to_asset.code,
            dest_issuer=self.convert_to_asset.issuer,
            dest_amount=config.CONVERTATION_AMOUNT,
            path=path,
        )

        if using_builder:
            return builder
        else:
            transaction_envelope = builder.build()
            transaction_envelope.sign(self.bribe_signer)
            return self.horizon.submit_transaction(transaction_envelope)

    def claim(self, bribe, using_builder=None):
        builder = using_builder or self._get_builder()

        balance = self.has_trustline(bribe.asset, self.bribe_address)
        if not bribe.asset.is_native() and not balance:
            builder.append_change_trust_op(
                asset_code=bribe.asset_code,
                asset_issuer=bribe.asset_issuer,
            )

        builder.append_claim_claimable_balance_op(bribe.claimable_balance_id)

        if using_builder:
            return builder
        else:
            transaction_envelope = builder.build()
            transaction_envelope.sign(self.bribe_signer)
            return self.horizon.submit_transaction(transaction_envelope)

    def payment(self, source, destination, asset, amount, using_builder=None):
        builder = using_builder or self._get_builder()

        builder.append_payment_op(
            destination=destination,
            asset_code=asset.code,
            asset_issuer=asset.issuer,
            source=source,
            amount=amount,
        )

        if using_builder:
            return builder
        else:
            transaction_envelope = builder.build()
            transaction_envelope.sign(self.bribe_signer)
            return self.horizon.submit_transaction(transaction_envelope)

    def claim_and_convert(self, bribe, using_builder=None):
        builder = using_builder or self._get_builder()

        builder = self.claim(bribe, using_builder=builder)
        builder = self.convert_asset(bribe, using_builder=builder)

        if using_builder:
            return builder
        else:
            transaction_envelope = builder.build()
            transaction_envelope.sign(self.bribe_signer)
            return self.horizon.submit_transaction(transaction_envelope)

    def claim_and_return(self, bribe, using_builder=None):
        builder = using_builder or self._get_builder()

        builder = self.claim(bribe, using_builder=builder)
        builder = self.payment(self.bribe_address, bribe.sponsor, bribe.asset, bribe.amount, using_builder=builder)

        if using_builder:
            return builder
        else:
            transaction_envelope = builder.build()
            transaction_envelope.sign(self.bribe_signer)
            return self.horizon.submit_transaction(transaction_envelope)
