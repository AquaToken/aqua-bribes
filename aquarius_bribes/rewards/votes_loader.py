from datetime import timedelta
from decimal import ROUND_DOWN, Decimal

from django.conf import settings
from django.db import IntegrityError, models

import requests

from aquarius_bribes.rewards.models import ClaimableBalance, VoteSnapshot
from aquarius_bribes.utils.assets import get_asset_string, parse_asset_string


class VotesLoader(object):
    def __init__(self, market_key, snapshot_time, base_url='https://voting-tracker.aqua.network'):
        self.market_key = market_key
        self.snapshot_time = snapshot_time
        self.base_url = base_url

    def _get_page(self, page, page_limit: int = 200):
        response = requests.get(
            '{}/api/market-keys/{}/votes/?limit={}&timestamp={}&page={}'.format(
                self.base_url, self.market_key, page_limit, self.snapshot_time.strftime("%s"), page,
            )
        )
        return response.json().get('results', [])

    def process_vote(self, vote):
        return VoteSnapshot(
            snapshot_time=self.snapshot_time,
            votes_value=vote['votes_value'],
            voting_account=vote['voting_account'],
            market_key_id=self.market_key,
        )

    def _get_delegated_asset_filter(self):
        asset_filter = models.Q()
        for _, delegated_asset in settings.DELEGATABLE_ASSETS:
            asset_filter |= models.Q(
                asset_code=delegated_asset.code,
                asset_issuer=delegated_asset.issuer,
            )
        return asset_filter

    def _get_delegatable_asset_filter(self):
        asset_filter = models.Q()
        for asset, _ in settings.DELEGATABLE_ASSETS:
            asset_filter |= models.Q(
                asset_code=asset.code,
                asset_issuer=asset.issuer,
            )
        return asset_filter

    def has_delegated_votes(self, voting_account):
        asset_filter = self._get_delegated_asset_filter()

        date = self.snapshot_time.replace(hour=0)
        return ClaimableBalance.objects.filter(
            loaded_at__gte=date, loaded_at__lt=date + timedelta(days=1),
        ).filter(owner=voting_account).filter(
            asset_filter,
        ).filter(claimants__destination=self.market_key).exists()

    def process_delegated_vote(self, voting_account, votes_value):
        votes = []
        votes_value = Decimal(votes_value)

        asset_filter = self._get_delegatable_asset_filter()

        date = self.snapshot_time.replace(hour=0)
        delegated_votes = ClaimableBalance.objects.filter(
            loaded_at__gte=date, loaded_at__lt=date + timedelta(days=1),
        ).filter(
            asset_filter,
        ).filter(claimants__destination=settings.DELEGATE_MARKER).filter(claimants__destination=voting_account)
        total_delegated_votes = delegated_votes.aggregate(total_votes=models.Sum('amount'))['total_votes']

        votes.append(
            VoteSnapshot(
                snapshot_time=self.snapshot_time,
                votes_value=votes_value,
                voting_account=voting_account,
                market_key_id=self.market_key,
                is_delegated=False,
                has_delegation=True,
            )
        )

        if total_delegated_votes and votes_value > total_delegated_votes:
            votes.append(
                VoteSnapshot(
                    snapshot_time=self.snapshot_time,
                    votes_value=votes_value - total_delegated_votes,
                    voting_account=voting_account,
                    market_key_id=self.market_key,
                    is_delegated=False,
                    has_delegation=False,
                )
            )

        for delegated_vote in delegated_votes:
            votes.append(
                VoteSnapshot(
                    snapshot_time=self.snapshot_time,
                    votes_value=Decimal(
                        delegated_vote.amount,
                    ).quantize(
                        Decimal('0.0000001'), rounding=ROUND_DOWN,
                    ),
                    voting_account=delegated_vote.owner,
                    market_key_id=self.market_key,
                    is_delegated=True,
                )
            )

        return votes

    def save_all_items(self, processed):
        try:
            VoteSnapshot.objects.bulk_create(processed, batch_size=5000)
        except IntegrityError:
            for item in processed:
                try:
                    item.save()
                except IntegrityError:
                    pass

    def load_votes(self):
        page = 1
        votes = self._get_page(page)

        while votes:
            parsed_votes = []
            for vote in votes:
                # if vote['asset'] in delegated_assets:
                if self.has_delegated_votes(vote['voting_account']):
                    parsed_votes += self.process_delegated_vote(
                        vote['voting_account'],
                        vote['votes_value'],
                    )
                else:
                    parsed_votes.append(
                        self.process_vote(vote)
                    )

            self.save_all_items(parsed_votes)

            page += 1
            votes = self._get_page(page)
