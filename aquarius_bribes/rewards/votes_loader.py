from django.db import IntegrityError

import requests

from aquarius_bribes.rewards.models import VoteSnapshot
from aquarius_bribes.bribes.utils import get_horizon


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
                parsed_votes.append(
                    self.process_vote(vote)
                )

            self.save_all_items(parsed_votes)

            page += 1
            votes = self._get_page(page)
