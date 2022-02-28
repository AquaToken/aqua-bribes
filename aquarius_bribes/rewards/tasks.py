from django.conf import settings
from django.core.cache import cache
from django.db import models
from django.utils import timezone

from datetime import timedelta
from decimal import Decimal, ROUND_UP
from stellar_sdk import Asset

from aquarius_bribes.bribes.models import AggregatedByAssetBribe
from aquarius_bribes.rewards.votes_loader import VotesLoader
from aquarius_bribes.rewards.models import AssetHolderBalanceSnapshot, VoteSnapshot
from aquarius_bribes.rewards.reward_payer import RewardPayer
from aquarius_bribes.rewards.trustees_loader import TrusteesLoader
from aquarius_bribes.rewards.utils import SecuredWallet
from aquarius_bribes.taskapp import app as celery_app


DEFAULT_REWARD_PERIOD = timedelta(hours=24)
PAYREWARD_TIME_LIMIT = timedelta(minutes=20)
LOAD_VOTES_TASK_ACTIVE_KEY = 'LOAD_VOTES_TASK_ACTIVE_KEY'
LOAD_VOTES_TASK_ACTIVE_TIMEOUT = timedelta(hours=2).total_seconds()


@celery_app.task(ignore_result=True, soft_time_limit=60 * 20, time_limit=60 * 30)
def task_run_load_votes():
    hour = random.randint(0, 5)
    task_load_votes.apply_async(countdown=2 * hour * timedelta(hours=1).total_seconds())


@celery_app.task(ignore_result=True, soft_time_limit=60 * 60 * 2, time_limit=60 * (60 * 2 + 5))
def task_load_votes(snapshot_time=None):
    cache.set(LOAD_VOTES_TASK_ACTIVE_KEY, True, LOAD_VOTES_TASK_ACTIVE_TIMEOUT)

    if snapshot_time is None:
        snapshot_time = timezone.now()
        snapshot_time = snapshot_time.replace(minute=0, second=0, microsecond=0)

    task_make_trustees_snapshot()

    markets_with_active_bribes = AggregatedByAssetBribe.objects.filter(
        start_at__lte=snapshot_time, stop_at__gt=snapshot_time,
    ).values_list('market_key', flat=True).distinct()

    for market_key in markets_with_active_bribes:
        loader = VotesLoader(market_key, snapshot_time)
        loader.load_votes()

    cache.set(LOAD_VOTES_TASK_ACTIVE_KEY, False, LOAD_VOTES_TASK_ACTIVE_TIMEOUT)


@celery_app.task(ignore_result=True, soft_time_limit=60 * 30, time_limit=60 * 35)
def task_make_trustees_snapshot(snapshot_time=None):
    if snapshot_time is None:
        snapshot_time = timezone.now()

    markets_with_active_bribes = AggregatedByAssetBribe.objects.filter(
        start_at__lte=snapshot_time, stop_at__gt=snapshot_time,
    )

    assets = set()
    for bribe in markets_with_active_bribes:
        assets.add((bribe.asset_code, bribe.asset_issuer))

    for asset_data in assets:
        if not (asset_data[0] == Asset.native().code and asset_data[1] == ''):
            asset = Asset(code=asset_data[0], issuer=asset_data[1])

            loader = TrusteesLoader(asset)
            loader.make_balances_spanshot()


@celery_app.task(ignore_result=True, soft_time_limit=PAYREWARD_TIME_LIMIT.total_seconds(), time_limit=60 * 25)
def task_pay_rewards(snapshot_time=None, reward_period=DEFAULT_REWARD_PERIOD):
    if cache.get(LOAD_VOTES_TASK_ACTIVE_KEY, False):
        return

    stop_at = timezone.now() + PAYREWARD_TIME_LIMIT

    if snapshot_time is None:
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
        votes = VoteSnapshot.objects.filter(
             snapshot_time__date=snapshot_time.date(), market_key=bribe.market_key,
        )

        if bribe.asset.type != Asset.native().type:
            votes = votes.filter(
                account__in=AssetHolderBalanceSnapshot.objects.filter(
                    created_at__date=snapshot_time.date(),
                    asset_code=bribe.asset_code,
                    asset_issuer=bribe.asset_issuer,
                ).values_list('account'),
            )

        reward_amount = bribe.daily_amount * Decimal(reward_period.total_seconds() / (24 * 3600))
        reward_payer = RewardPayer(bribe, reward_wallet, bribe.asset, reward_amount, stop_at=stop_at)
        reward_payer.pay_reward(votes)
