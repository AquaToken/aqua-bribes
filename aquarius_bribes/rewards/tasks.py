from django.conf import settings
from django.utils import timezone

from datetime import timedelta
from decimal import Decimal, ROUND_UP
from stellar_sdk import Asset

from aquarius_bribes.bribes.models import AggregatedByAssetBribe
from aquarius_bribes.rewards.votes_loader import VotesLoader
from aquarius_bribes.rewards.models import VoteSnapshot
from aquarius_bribes.rewards.reward_payer import RewardPayer
from aquarius_bribes.rewards.utils import SecuredWallet
from aquarius_bribes.taskapp import app as celery_app


DEFAULT_REWARD_PERIOD = timedelta(hours=24)
PAYREWARD_TIME_LIMIT = timedelta(minutes=20)


@celery_app.task(ignore_result=True, soft_time_limit=60 * 20, time_limit=60 * 30)
def task_run_load_votes():
    hour = random.randint(0, 23)
    task_load_votes.apply_async(countdown=hour * 60 * 60)


@celery_app.task(ignore_result=True, soft_time_limit=60 * 10, time_limit=60 * 15)
def task_load_votes():
    snapshot_time = timezone.now()
    snapshot_time = snapshot_time.replace(minute=0, second=0, microsecond=0)

    markets_with_active_bribes = AggregatedByAssetBribe.objects.filter(
        start_at__lte=snapshot_time, stop_at__gt=snapshot_time,
    ).values_list('market_key', flat=True).distinct()

    for market_key in markets_with_active_bribes:
        loader = VotesLoader(market_key, snapshot_time)
        loader.load_votes()

    # task_pay_rewards.delay()


@celery_app.task(ignore_result=True, soft_time_limit=PAYREWARD_TIME_LIMIT.total_seconds(), time_limit=60 * 25)
def task_pay_rewards(reward_period=DEFAULT_REWARD_PERIOD):
    return
    stop_at = timezone.now() + PAYREWARD_TIME_LIMIT

    snapshot_time = timezone.now()
    snapshot_time = snapshot_time.replace(minute=0, second=0, microsecond=0)

    reward_wallet = SecuredWallet(
        public_key=settings.BRIBE_WALLET_ADDRESS,
        secret=settings.BRIBE_WALLET_SIGNER,
    )

    active_bribes = AggregatedByAssetBribe.objects.filter(
        start_at__lte=snapshot_time, stop_at__gt=snapshot_time,
    )

    for bribe in active_bribes:
        votes = VoteSnapshot.objects.filter(snapshot_time=snapshot_time, market_key=bribe.market_key)
        total_votes = votes.aggregate(total_votes=models.Sum("votes_value"))["total_votes"]

        reward_amount = bribe.daily_amount * Decimal(reward_period.total_seconds() / (24 * 3600))
        reward_payer = RewardPayer(bribe, reward_wallet, bribe.asset, reward_amount, stop_at=stop_at)
        reward_payer.pay_reward(votes)
