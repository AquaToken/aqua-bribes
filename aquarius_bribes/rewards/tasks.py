from django.conf import settings
from django.utils import timezone

from decimal import Decimal, ROUND_UP

from aquarius_bribes.bribes.models import Bribe
from aquarius_bribes.rewards.votes_loader import VotesLoader
from aquarius_bribes.rewards.models import VoteSnapshot
from aquarius_bribes.rewards.reward_payer import RewardPayer
from aquarius_bribes.rewards.utils import SecuredWallet
from aquarius_bribes.taskapp import app as celery_app


@celery_app.task(ignore_result=True)
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


@celery_app.task(ignore_result=True)
def task_pay_rewards():
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

        reward_payer = RewardPayer(bribe, reward_wallet)
        reward_payer.pay_reward(votes)
