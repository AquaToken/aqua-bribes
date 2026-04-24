from datetime import datetime, time, timedelta
from decimal import ROUND_UP, Decimal

from django.db import models
from django.utils import timezone

from stellar_sdk import Asset

from aquarius_bribes.rewards.models import AssetHolderBalanceSnapshot, VoteSnapshot


def get_asset_holders(asset_code, asset_issuer, snapshot_date):
    """Accounts that held a trustline for the asset on the given day."""
    day_start = timezone.make_aware(datetime.combine(snapshot_date, time.min))
    day_end = day_start + timedelta(days=1)
    return set(
        AssetHolderBalanceSnapshot.objects.filter(
            created_at__gte=day_start,
            created_at__lt=day_end,
            asset_code=asset_code,
            asset_issuer=asset_issuer,
        ).values_list('account', flat=True)
    )


def get_payable_votes(bribe, snapshot_date, reward_amount=None, asset_holder_cache=None):
    """
    Return (votes_qs, total_votes_pre_dust) — shared definition of the payable
    set used by task_pay_rewards, reconcile, and monitoring.

    Filters (identical to the inline chain in task_pay_rewards):
      1. VoteSnapshot(market_key=bribe.market_key, snapshot_time=snapshot_date)
      2. For non-native bribes: voting_account must have an
         AssetHolderBalanceSnapshot for the bribe asset on that UTC day
         (trustline requirement). Holders are fetched once via the
         (asset_code, asset_issuer, created_at) composite index and applied
         to VoteSnapshot as ``voting_account = ANY(%s)``. This keeps the
         queryset filter structural (no subquery join), which avoids a
         Nested Loop Semi Join on large holder sets.
      3. has_delegation=False (delegators routed through delegatee).
      4. (optional) If reward_amount is given: dust filter
         votes_value >= ceil(1e-7 * total_votes_pre_dust / reward_amount)
         — exactly matches RewardPayer._exclude_small_votes.

    total_votes_pre_dust is the sum over the PRE-dust set (post-market,
    post-trustline, post-delegation). It is what reward arithmetic must
    use as the denominator — computing over the post-dust queryset
    inflates per-recipient reward values.

    asset_holder_cache: optional ``{(asset_code, asset_issuer, date): set}``
    dict; when supplied, callers that walk many bribes for the same date
    reuse the holder set across invocations.
    """
    votes = VoteSnapshot.objects.filter(
        market_key=bribe.market_key,
        snapshot_time=snapshot_date,
    )

    if bribe.asset.type != Asset.native().type:
        cache_key = (bribe.asset_code, bribe.asset_issuer, snapshot_date)
        if asset_holder_cache is not None and cache_key in asset_holder_cache:
            accounts = asset_holder_cache[cache_key]
        else:
            accounts = get_asset_holders(
                bribe.asset_code, bribe.asset_issuer, snapshot_date,
            )
            if asset_holder_cache is not None:
                asset_holder_cache[cache_key] = accounts

        if not accounts:
            return VoteSnapshot.objects.none(), None

        votes = votes.extra(
            where=['voting_account = ANY(%s)'],
            params=[list(accounts)],
        )

    votes = votes.exclude(has_delegation=True)

    total_votes_pre_dust = votes.aggregate(
        total=models.Sum('votes_value'),
    )['total']

    if reward_amount is not None and total_votes_pre_dust and total_votes_pre_dust > 0:
        min_votes_value = Decimal(
            Decimal('0.0000001') * total_votes_pre_dust / Decimal(reward_amount),
        ).quantize(Decimal('0.0000001'), rounding=ROUND_UP)
        votes = votes.filter(votes_value__gte=min_votes_value)

    return votes, total_votes_pre_dust
