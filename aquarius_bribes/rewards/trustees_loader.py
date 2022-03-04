from typing import Dict, List

from django.core.cache import cache

from stellar_sdk import Asset
from stellar_sdk.exceptions import (
    BadResponseError,
    ConnectionError,
)

from aquarius_bribes.rewards.models import AssetHolderBalanceSnapshot
from aquarius_bribes.bribes.utils import get_horizon


class TrusteesLoader(object):
    def __init__(
        self, asset: Asset, last_id_cache_key: str = None, last_id_cache_timeout: int = 60 * 60 * 12,
    ):
        self.asset = asset
        self.horizon = get_horizon()

        if not last_id_cache_key:
            last_id_cache_key = '{0}:{1}_trustees_loader'.format(self.asset.code, self.asset.issuer)
        self.last_id_cache_key = last_id_cache_key
        self.last_id_cache_timeout = last_id_cache_timeout

    def load_last_event_id(self) -> str:
        paging_token = cache.get(self.last_id_cache_key, None)

        if paging_token:
            return paging_token

    def save_last_event_id(self, last_id: str):
        cache.set(self.last_id_cache_key, last_id, self.last_id_cache_timeout)

    def _get_page(self, page_limit: int = 200) -> List[Dict]:
        try:
            return self.horizon.accounts().for_asset(
                Asset(code=self.asset.code, issuer=self.asset.issuer),
            ).cursor(
                self.load_last_event_id(),
            ).limit(page_limit).order(
                desc=False,
            ).call()['_embedded']['records']
        except (BadResponseError, ConnectionError):
            return None

    def make_balances_spanshot(self):
        accounts_page = self._get_page()
        processed_accounts = []
        while accounts_page or accounts_page is None:
            if accounts_page is not None:
                for account in accounts_page:
                    processed_accounts.append(self._process_account(account))

                self.save_last_event_id(processed_accounts[-1].account)

            accounts_page = self._get_page()

        AssetHolderBalanceSnapshot.objects.bulk_create(processed_accounts, batch_size=5000)

    def _process_account(self, account: Dict) -> AssetHolderBalanceSnapshot:
        balance = next(
            (
                x for x in account['balances']
                if x.get('asset_code', None) == self.asset.code and x.get('asset_issuer', None) == self.asset.issuer
            ),
            None,
        )
        return AssetHolderBalanceSnapshot(
            account=account['account_id'],
            asset_code=self.asset.code,
            asset_issuer=self.asset.issuer or '',
            balance=balance['balance'],
        )
