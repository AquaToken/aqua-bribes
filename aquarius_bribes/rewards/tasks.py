from django.conf import settings
from django.utils import timezone

from datetime import timedelta
from decimal import Decimal, ROUND_UP
from stellar_sdk import Asset

from aquarius_bribes.bribes.models import Bribe
from aquarius_bribes.rewards.votes_loader import VotesLoader
from aquarius_bribes.rewards.models import VoteSnapshot
from aquarius_bribes.rewards.reward_payer import RewardPayer
from aquarius_bribes.rewards.utils import SecuredWallet
from aquarius_bribes.taskapp import app as celery_app


DEFAULT_REWARD_PERIOD = timedelta(hours=1)
PAYREWARD_TIME_LIMIT = timedelta(minutes=20)


@celery_app.task(ignore_result=True, soft_time_limit=60 * 10, time_limit=60 * 15)
def task_load_votes():
    snapshot_time = timezone.now()
    snapshot_time = snapshot_time.replace(minute=0, second=0, microsecond=0)

    markets_with_active_bribes = Bribe.objects.filter(
        start_at__lte=snapshot_time, stop_at__gt=snapshot_time,
        status=Bribe.STATUS_ACTIVE,
    ).values_list('market_key', flat=True).distinct()

    for market_key in markets_with_active_bribes:
        loader = VotesLoader(market_key, snapshot_time)
        loader.load_votes()

    task_pay_rewards.delay()


@celery_app.task(ignore_result=True, soft_time_limit=PAYREWARD_TIME_LIMIT.total_seconds(), time_limit=60 * 25)
def task_pay_rewards(reward_period=DEFAULT_REWARD_PERIOD):
    stop_at = timezone.now() + PAYREWARD_TIME_LIMIT
    aqua = Asset(code=settings.REWARD_ASSET_CODE, issuer=settings.REWARD_ASSET_ISSUER)

    snapshot_time = timezone.now()
    snapshot_time = snapshot_time.replace(minute=0, second=0, microsecond=0)

    markets_with_active_bribes = Bribe.objects.filter(
        start_at__lte=snapshot_time, stop_at__gt=snapshot_time, status=Bribe.STATUS_ACTIVE,
    ).values_list('market_key', flat=True).distinct()

    for market_key in markets_with_active_bribes:
        votes = VoteSnapshot.objects.filter(snapshot_time=snapshot_time, market_key=market_key)
        total_votes = votes.aggregate(total_votes=models.Sum("votes_value"))["total_votes"]

        smallest_rewarded_votes_amount = (total_votes * Decimal("0.0000001"))
        smallest_rewarded_votes_amount = smallest_rewarded_votes_amount.quantize(
            Decimal("0.0000001"), rounding=ROUND_UP,
        )
        votes = votes.filter(votes_value__gte=smallest_rewarded_votes_amount)

        reward_wallet = SecuredWallet(
            public_key=settings.BRIBE_WALLET_ADDRESS,
            secret=settings.BRIBE_WALLET_SIGNER,
        )

        reward_payer = RewardPayer(bribe, reward_wallet, aqua, reward_period, stop_at=stop_at)
        reward_payer.pay_reward(votes)
