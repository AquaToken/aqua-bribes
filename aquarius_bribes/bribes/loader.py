from django.conf import settings
from django.core.cache import cache

from datetime import timedelta
from dateutil.parser import parse as date_parse
from stellar_sdk import Asset

from aquarius_bribes.bribes.models import Bribe, MarketKey
from aquarius_bribes.bribes.utils import get_horizon


class BribesLoader(object):
    def __init__(self, account, signer, last_id_cache_timeout: int = 60 * 60 * 12):
        self.account = account
        self.signer = signer
        self.horizon = get_horizon()
        self.last_id_cache_key = None
        self.last_id_cache_timeout = last_id_cache_timeout

    def load_last_event_id(self) -> str:
        paging_token = cache.get(self.last_id_cache_key, None)

        if paging_token:
            return paging_token

    def load_last_event_id(self) -> str:
        paging_token = cache.get(self.last_id_cache_key, None)

        if paging_token:
            return paging_token

        last_saved_bribe = Bribe.objects.order_by('-created_at').first()

        if last_saved_bribe and last_saved_bribe.paging_token:
            return last_saved_bribe.paging_token

    def save_last_event_id(self, last_id: str):
        cache.set(self.last_id_cache_key, last_id, self.last_id_cache_timeout)

    def _get_page(self, page_limit: int = 200):
        return self.horizon.claimable_balances().for_claimant(
            self.account,
        ).limit(page_limit).cursor(
            self.load_last_event_id(),
        ).order(
            desc=False,
        ).call()['_embedded']['records']

    def _is_market_key_predicate_correct(self, predicate: dict):
        return predicate == {
            'not': {
                'unconditional': True,
            },
        }

    def _parse_bribe_predicate(self, predicate: dict):
        return predicate.get('not', {}).get('abs_before', None)

    def _get_asset_equivalent(self, amount, asset, to_asset):
        if asset == to_asset:
            return amount

        paths = horizon.strict_send_paths(
            source_amount=amount, destination=[to_asset], source_asset=asset
        ).call().get("_embedded", {}).get("records", [])

        if len(paths) == 0:
            return 0
        else:
            return paths[0]['destination_amount']

    def parse(self, bribe):
        amount = bribe['amount']
        sponsor = bribe['sponsor']
        claimants = bribe['claimants']
        claimable_balance_id = bribe['id']
        paging_token = bribe['paging_token']

        asset = bribe['asset']
        if asset == 'native':
            asset = Asset.native()
        else:
            asset = asset.split(':')
            asset = Asset(code=asset[0], issuer=asset[1])

        balance_created_at = bribe['last_modified_time']
        if len(claimants) != 2:
            raise Exception('Invalid claimants.')

        bribe_collector_claim, market_key_claim = sorted(
            claimants, key=lambda cl: cl['destination'] == self.account, reverse=True,
        )

        status = Bribe.STATUS_PENDING
        messages = []

        if bribe_collector_claim['destination'] != self.account:
            messages.append('Invalid predicate: no bribe account')

        if not self._is_market_key_predicate_correct(market_key_claim['predicate']):
            messages.append('Invalid predicate: market key predicate incorrect')

        unlock_time = self._parse_bribe_predicate(bribe_collector_claim['predicate'])
        if not unlock_time:
            messages.append('Invalid predicate: bribe account predicate incorrect time')

        try:
            balance_created_at = date_parse(balance_created_at)

            if unlock_time:
                unlock_time = date_parse(unlock_time)
        except ValueError:
            unlock_time = None
            balance_created_at = None
            messages.append('Invalid predicate: invalid time format')

        if len(messages) > 0 and unlock_time:
            status = Bribe.STATUS_PENDING_RETURN
        elif len(messages) > 0:
            status = Bribe.STATUS_INVALID

        market_key, _ = MarketKey.objects.get_or_create(market_key=market_key_claim['destination'])
        aqua = Asset(code=settings.REWARD_ASSET_CODE, issuer=settings.REWARD_ASSET_ISSUER)

        bribe = Bribe(
            asset_code=asset.code,
            asset_issuer=asset.issuer or '',
            sponsor=sponsor,
            market_key=market_key,
            amount=amount,
            claimable_balance_id=claimable_balance_id,
            paging_token=paging_token,
            created_at=balance_created_at,
            unlock_time=unlock_time,
            status=status,
            message='\n'.join(messages),
            aqua_total_reward_amount_equivalent=self._get_asset_equivalent(amount, asset, aqua),
        )

        bribe.update_active_period()
        return bribe

    def process_bribe(self, bribe):
        bribe_instance = self.parse(bribe)
        return bribe_instance

    def load_bribes(self):
        bribes = self._get_page()

        while bribes:
            parsed_bribes = []
            for bribe in bribes:
                parsed_bribes.append(
                    self.process_bribe(bribe)
                )

            Bribe.objects.bulk_create(parsed_bribes, batch_size=5000)
            self.save_last_event_id(parsed_bribes[-1].paging_token)

            bribes = self._get_page()
