import logging
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import ROUND_DOWN, Decimal
from typing import Dict, Iterator, List, Optional, Tuple

from django.core.cache import cache
from django.db import transaction
from django.utils import timezone

from stellar_sdk import Asset

from aquarius_bribes.bribes.models import AggregatedByAssetBribe
from aquarius_bribes.bribes.utils import get_horizon
from aquarius_bribes.rewards.eligibility import get_payable_votes
from aquarius_bribes.rewards.models import Payout, VoteSnapshot
from aquarius_bribes.rewards.tasks import PAY_REWARDS_FIX_DB_TTL

_REWARD_AMOUNT_QUANTUM = Decimal('0.0000000')

logger = logging.getLogger(__name__)

OpTuple = Tuple[str, str, str, str, Decimal]

# Sentinel values returned by _resolve_chain_only_candidate.
_CHAIN_ORPHAN = object()
_AMBIGUOUS = object()
_NO_ACTIVE_BRIBE = object()


class FixDbLockLost(Exception):
    """
    Raised when apply_fix_db detects mid-run that the cache lock
    (PAY_REWARDS_FIX_DB_ACTIVE_KEY) has been evicted or taken over by
    another operator. Aborting at this point prevents racing with a
    concurrent fix-db run.
    """


@dataclass
class ReconcileReport:
    from_date: date
    to_date: date
    bribe_wallet: str
    chain_payments_count: int = 0
    db_success_count: int = 0
    db_failed_count: int = 0
    matched: List[tuple] = field(default_factory=list)
    chain_only: List[tuple] = field(default_factory=list)
    db_only: List[tuple] = field(default_factory=list)
    ambiguous: List[tuple] = field(default_factory=list)
    missed: List[tuple] = field(default_factory=list)
    per_bribe_missed: dict = field(default_factory=dict)
    skipped_no_active_bribe: int = 0
    skipped_future_window_chain_op: int = 0

    @property
    def chain_orphan_count(self) -> int:
        return sum(1 for item in self.chain_only if item[6] is None)


def _parse_created_at(created_at_str: str) -> datetime:
    return datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))


def _normalize_asset(record: dict) -> Tuple[str, str]:
    if record.get("asset_type") == Asset.native().type:
        return Asset.native().code, ""
    return record.get("asset_code", ""), record.get("asset_issuer", "") or ""


def _iter_payment_ops(
    server, bribe_wallet, from_date, to_date
) -> Iterator[Tuple[dict, datetime, dict]]:
    # Walk newest → oldest so we can stop as soon as we cross below from_date.
    # Ascending without an initial cursor makes Horizon start from the account's
    # very first payment, which on a long-lived bribe wallet means reconciling a
    # one-day window forces paging through years of history.
    cursor = None
    join_supported: Optional[bool] = None
    tx_cache: Dict[str, dict] = {}

    while True:
        builder = server.payments().for_account(bribe_wallet).include_failed(False)

        if join_supported is not False:
            try:
                builder = builder.join("transactions")
                join_supported = True
            except (AttributeError, TypeError):
                join_supported = False

        builder = builder.limit(200).order(desc=True)
        if cursor:
            builder = builder.cursor(cursor)

        response = builder.call()
        records = response.get("_embedded", {}).get("records", [])
        if not records:
            return

        for record in records:
            created_at = _parse_created_at(record["created_at"])
            if created_at.date() > to_date:
                continue
            if created_at.date() < from_date:
                return
            if record.get("type") != "payment":
                continue
            if record.get("from") != bribe_wallet:
                continue

            tx_info = record.get("transaction", {})
            if join_supported is False:
                tx_hash = record.get("transaction_hash")
                if tx_hash not in tx_cache:
                    tx_cache[tx_hash] = (
                        server.transactions().transaction(tx_hash).call()
                    )
                tx_info = tx_cache[tx_hash]

            yield record, created_at, tx_info

        cursor = records[-1]["paging_token"]


def _pick_payout_for_op(
    payouts: List[Payout],
) -> Tuple[Optional[Payout], bool]:
    """
    Resolve 0..N candidate Payouts for one chain op.

    Returns (payout, ambiguous):
      - (None, False): no candidate — caller falls through to memo/bribe
        resolution.
      - (Payout, False): single best candidate. Success is preferred over
        failed; ties inside a status class are broken by vote_snapshot_id
        to keep the choice deterministic across runs.
      - (None, True): ambiguous — multiple SUCCESS candidates, or multiple
        FAILED with no SUCCESS. Caller routes to report.ambiguous so
        --fix-db skips rather than risk downgrading a real SUCCESS row.
    """
    if not payouts:
        return None, False

    ordered = sorted(payouts, key=lambda p: p.vote_snapshot_id or 0)
    success = [p for p in ordered if p.status == Payout.STATUS_SUCCESS]
    if len(success) >= 2:
        return None, True
    if len(success) == 1:
        return success[0], False

    failed = [p for p in ordered if p.status == Payout.STATUS_FAILED]
    if len(failed) >= 2:
        return None, True
    if len(failed) == 1:
        return failed[0], False

    return ordered[0], False


def _resolve_chain_only_candidate(
    destination: str,
    asset_code: str,
    asset_issuer: str,
    short_value: str,
    in_window_snapshot_times: list,
    amount: Optional[Decimal] = None,
):
    """
    Resolve a chain op with no matching Payout to a (bribe, vote_snapshot) pair.

    Strategy (per-date VoteSnapshot-first):
      1. Look up VoteSnapshots for the destination account on the candidate
         snapshot dates (chain_date and chain_date-1, intersected with the
         operator window). No market_key filter yet — the bribe is derived
         from the VoteSnapshot we find.
      2. No VoteSnapshot → _CHAIN_ORPHAN.
      3. For EACH date the voter has a VS on, find the memo+asset+short_value
         bribe active that day. Per-date candidate = (bribe, [matching VSs
         filtered by bribe.market_key]).
      4. One date has candidates → resolve inside that date (len(matching)==1
         direct; >1 → amount-match, then _AMBIGUOUS if zero/multiple).
      5. Multiple dates have candidates with DIFFERENT bribes → _AMBIGUOUS
         (weekly-rollover midnight-cross: bribe_A stops D 00:00, bribe_B
         starts D 00:00, tx for bribe_A submitted D-1 23:58 lands D 00:05).
      6. Multiple dates have candidates with the SAME bribe (ordinary
         recurring voter on a multi-day bribe) → amount-replay across all
         per-date candidates. Return when amount uniquely identifies one VS;
         _AMBIGUOUS when amount is absent, doesn't match any, or matches >1.

    VoteSnapshot is the date-of-eligibility source of truth. Picking the
    bribe after the snapshot — constrained to the snapshot's exact day via
    the half-open window — avoids weekly-rollover collisions where two
    consecutive weekly bribes with the same (asset, market_key) both fall
    inside a naive ±1 day bribe-candidate window.
    """
    if not in_window_snapshot_times:
        return _CHAIN_ORPHAN

    vote_snapshots = list(
        VoteSnapshot.objects.filter(
            voting_account=destination,
            snapshot_time__in=in_window_snapshot_times,
            has_delegation=False,
        )
    )

    if not vote_snapshots:
        return _CHAIN_ORPHAN

    def _bribe_candidates_on(pivot_date):
        pivot_start_local = timezone.make_aware(datetime.combine(pivot_date, time.min))
        pivot_end_local = timezone.make_aware(
            datetime.combine(pivot_date + timedelta(days=1), time.min)
        )
        raw = AggregatedByAssetBribe.objects.filter(
            start_at__lt=pivot_end_local,
            stop_at__gt=pivot_start_local,
            asset_code=asset_code,
            asset_issuer=asset_issuer or "",
        ).select_related("market_key")
        return [
            b for b in raw
            if b.market_key and b.market_key.short_value == short_value
        ]

    # Resolve per date: for each snapshot date the voter has a VS on,
    # find the memo+asset+short_value-matching bribe active on that day
    # (the half-open `__lt=pivot_end` / `__gt=pivot_start` window ensures
    # back-to-back weekly bribes never overlap a single day). Each date
    # yields its own (bribe, [matching VSs]) tuple where matching is the
    # VS subset filtered by bribe.market_key. Routing decisions are made
    # over the full set of per-date candidates — this is the only way to
    # distinguish a weekly-rollover midnight-cross (different bribes on
    # D and D-1) from a single multi-day bribe that the voter votes on
    # both days.
    per_date_candidates: dict = {}
    for d in sorted({vs.snapshot_time for vs in vote_snapshots}):
        vss_on_d = [vs for vs in vote_snapshots if vs.snapshot_time == d]
        bribes_on_d = _bribe_candidates_on(d)
        if not bribes_on_d:
            continue
        if len(bribes_on_d) > 1:
            return _AMBIGUOUS
        bribe_on_d = bribes_on_d[0]
        matching_on_d = [
            vs for vs in vss_on_d
            if vs.market_key_id == bribe_on_d.market_key_id
        ]
        if matching_on_d:
            per_date_candidates[d] = (bribe_on_d, matching_on_d)

    if not per_date_candidates:
        return _NO_ACTIVE_BRIBE

    def _amount_match_in_date(bribe, matching, pivot_date):
        _, total = get_payable_votes(bribe, pivot_date)
        if not total or total <= 0:
            return []
        out = []
        for vs in matching:
            expected = (
                Decimal(bribe.daily_amount) * Decimal(vs.votes_value)
                / Decimal(total)
            ).quantize(_REWARD_AMOUNT_QUANTUM, rounding=ROUND_DOWN)
            if expected == amount:
                out.append(vs)
        return out

    if len(per_date_candidates) == 1:
        (d, (bribe, matching)) = next(iter(per_date_candidates.items()))
        if len(matching) == 1:
            return bribe, matching[0]
        # Multi-VS on a single date — delegatee with >1 incoming delegation
        # on the same market + same day (each producing a
        # `has_delegation=False` snapshot). Replay RewardPayer's arithmetic
        # to pick the voter the tx actually paid.
        if amount is None:
            return _AMBIGUOUS
        resolved = _amount_match_in_date(bribe, matching, d)
        if len(resolved) == 1:
            return bribe, resolved[0]
        return _AMBIGUOUS

    # Multiple dates have candidates. If the bribes differ, this is a
    # weekly-rollover midnight-cross shape (bribe_A ends D 00:00,
    # bribe_B starts D 00:00). Amount match across different bribes would
    # need separate payable-votes totals per bribe; the safer answer is
    # to route to AMBIGUOUS for operator inspection.
    distinct_bribe_ids = {bribe.id for (bribe, _) in per_date_candidates.values()}
    if len(distinct_bribe_ids) > 1:
        return _AMBIGUOUS

    # Same bribe across multiple dates (ordinary recurring voter on a
    # single multi-day bribe). The chain op could be for any of the dates
    # the voter was eligible, including a midnight-cross. Replay the
    # expected amount per date × per VS; only return a concrete
    # (bribe, VS) pair when amount uniquely identifies it.
    bribe = next(iter(per_date_candidates.values()))[0]
    if amount is None:
        return _AMBIGUOUS
    resolved: list = []
    for d, (_, matching) in per_date_candidates.items():
        resolved.extend(_amount_match_in_date(bribe, matching, d))
    if len(resolved) == 1:
        return bribe, resolved[0]
    return _AMBIGUOUS


def reconcile_bribe_payouts(
    from_date, to_date, bribe_wallet, server=None
) -> ReconcileReport:
    server = server or get_horizon()
    report = ReconcileReport(
        from_date=from_date, to_date=to_date, bribe_wallet=bribe_wallet
    )

    db_success_qs = Payout.objects.filter(
        status=Payout.STATUS_SUCCESS,
        stellar_transaction_id__gt="",
        vote_snapshot__snapshot_time__range=[from_date, to_date],
    ).select_related("vote_snapshot", "bribe")
    db_failed_qs = Payout.objects.filter(
        status=Payout.STATUS_FAILED,
        vote_snapshot__snapshot_time__range=[from_date, to_date],
    )
    report.db_success_count = db_success_qs.count()
    report.db_failed_count = db_failed_qs.count()

    # Counter (not set) so two chain ops sharing the same 5-tuple both
    # account for their matching Payouts. With a set, a second Payout
    # that shares (tx, dest, asset, amount) would wrongly be treated as
    # db_only once the first consumes the single "seen" entry.
    seen_chain_op_tuples: Counter = Counter()
    # Tuples whose Payout-first lookup returned >1 candidate. Any Payout
    # with one of these tuples must be routed to AMBIGUOUS in the
    # db_success_qs walk — downgrading one of several genuinely-colliding
    # SUCCESS rows would corrupt data.
    ambiguous_op_tuples: set = set()
    # Key by vote_snapshot_id (not voting_account): a delegatee can hold
    # multiple same-date VoteSnapshots with has_delegation=False, and a
    # per-account key would hide partial payout losses for the others.
    matched_chain_votesnapshots_by_bribe_date = defaultdict(set)

    # Walk the chain with a ±1 day pad: task_pay_rewards for snapshot_date=D
    # can submit the tx after midnight, so its on-chain created_at falls on
    # D+1 while the Payout still has vote_snapshot.snapshot_time=D. Without
    # the pad, seen_chain_op_tuples would miss that op and the downstream
    # db_only walk would classify a valid success Payout as drift; --fix-db
    # would then downgrade it to FAILED.
    chain_from = from_date - timedelta(days=1)
    chain_to = to_date + timedelta(days=1)

    for record, created_at, tx_info in _iter_payment_ops(
        server, bribe_wallet, chain_from, chain_to
    ):
        tx_hash = record.get("transaction_hash") or tx_info.get("hash", "")
        destination = record.get("to", "")
        asset_code, asset_issuer = _normalize_asset(record)
        amount = Decimal(record.get("amount", "0"))
        op_tuple: OpTuple = (tx_hash, destination, asset_code, asset_issuer, amount)
        seen_chain_op_tuples[op_tuple] += 1

        memo_type = tx_info.get("memo_type", "none")
        memo = tx_info.get("memo", "") or ""
        if memo_type != "text" or not memo.startswith("Bribe: "):
            continue

        report.chain_payments_count += 1
        short_value = memo[len("Bribe: ") :].strip()

        # Payout-first lookup. The filter ties a chain op to a Payout via
        #   (stellar_transaction_id, vote_snapshot.voting_account,
        #    vote_snapshot.snapshot_time ∈ {chain_date, chain_date - 1}
        #                                  ∩ [from_date, to_date],
        #    asset_code, asset_issuer, reward_amount)
        # The snapshot_time narrowing keeps the 5-tuple unique when two
        # delegators contribute the same votes_value to the same delegatee
        # in the same tx, and chain_date - 1 covers midnight-cross:
        # task_pay_rewards for snapshot_time=D can land on-chain at D+1
        # 00:0x. The intersection with [from_date, to_date] is required
        # so a chain op outside the operator's requested window can never
        # resolve to a Payout whose snapshot_time is outside that window
        # — otherwise --fix-db would mutate past-date rows the operator
        # never asked for.
        chain_date_for_payout = created_at.date()
        in_window_snapshot_times = [
            d
            for d in (
                chain_date_for_payout - timedelta(days=1),
                chain_date_for_payout,
            )
            if from_date <= d <= to_date
        ]
        if not in_window_snapshot_times:
            # Chain op is outside [from_date, to_date] window on both
            # candidate snapshot dates. Its Counter entry still helps the
            # db_only walk dedupe coincidental 5-tuple collisions with
            # in-window Payouts, but we must not match/mutate anything.
            continue

        payout, payout_ambiguous = _pick_payout_for_op(
            list(
                Payout.objects.filter(
                    stellar_transaction_id=tx_hash,
                    vote_snapshot__voting_account=destination,
                    vote_snapshot__snapshot_time__in=in_window_snapshot_times,
                    asset_code=asset_code,
                    asset_issuer=asset_issuer or "",
                    reward_amount=amount,
                ).select_related("vote_snapshot", "bribe__market_key")
            )
        )

        if payout_ambiguous:
            # Multiple SUCCESS or multiple FAILED Payouts match the same
            # chain op — --fix-db must not guess which one to upgrade or
            # leave alone. Route to ambiguous so an operator can resolve,
            # and remember the tuple so the db_success_qs walk below also
            # skips those rows.
            ambiguous_op_tuples.add(op_tuple)
            report.ambiguous.append(
                (
                    tx_hash,
                    destination,
                    asset_code,
                    asset_issuer,
                    amount,
                    memo,
                    created_at,
                    [],
                )
            )
            continue

        if payout is not None:
            bribe = payout.bribe
            snapshot_time = payout.vote_snapshot.snapshot_time
            matched_chain_votesnapshots_by_bribe_date[(bribe.id, snapshot_time)].add(
                payout.vote_snapshot_id
            )
            if payout.status == Payout.STATUS_SUCCESS:
                report.matched.append(
                    (
                        tx_hash,
                        destination,
                        asset_code,
                        asset_issuer,
                        amount,
                        bribe.id,
                        payout.vote_snapshot_id,
                        payout.id,
                    )
                )
            else:
                report.chain_only.append(
                    (
                        tx_hash,
                        destination,
                        asset_code,
                        asset_issuer,
                        amount,
                        bribe.id,
                        payout.vote_snapshot_id,
                        memo,
                        created_at,
                        payout.id,
                    )
                )
            continue

        # X6 guard: chain ops landing AFTER the operator's `--to` date
        # (typical case: `--to D-1` runs on day D; the right pad
        # `chain_to = to_date + 1` brings today's chain ops into this loop).
        # `in_window_snapshot_times` for these ops collapses to `[to_date]`
        # — but the chain op was actually for `VS_today`, not `VS_to_date`.
        # The Payout-first lookup misses today's real Payout because its
        # `snapshot_time=today` is excluded from the candidate list, and
        # the resolver's single-VS branch would silently return
        # `(bribe, VS_to_date)` without amount-checking, letting
        # `apply_fix_db.get_or_create` fabricate a SUCCESS Payout that
        # misattributes today's tx_hash to yesterday's snapshot.
        # Skip the resolver entirely for these chain ops: they belong to
        # a future operator window and will be reconciled correctly when
        # `--to >= chain_date`. If a real Payout already exists past the
        # operator window, `continue` silently (it's not in scope for
        # this run); otherwise count via dedicated `skipped_future_window_chain_op`
        # so the operator sees today's chain activity was acknowledged.
        if chain_date_for_payout > to_date:
            report.skipped_future_window_chain_op += 1
            continue

        # No Payout — resolve via VoteSnapshot-first strategy (X5):
        # VoteSnapshot carries the date the voter was eligible; that date
        # is the authoritative pivot for picking which bribe was active.
        # Resolving bribe candidates using only the chain op's created_at
        # with a ±1 day pad causes weekly-rollover ambiguity: two consecutive
        # weekly bribes with the same (asset, market_key, short_value) both
        # fall inside the ±1 day window of a Monday 00:0x op, routing it
        # incorrectly to AMBIGUOUS and blocking --fix-db from rebuilding it.
        result = _resolve_chain_only_candidate(
            destination=destination,
            asset_code=asset_code,
            asset_issuer=asset_issuer,
            short_value=short_value,
            in_window_snapshot_times=in_window_snapshot_times,
            amount=amount,
        )

        if result is _NO_ACTIVE_BRIBE:
            report.skipped_no_active_bribe += 1
            logger.warning(
                "No active bribe matches memo+asset for tx %s", tx_hash
            )
            continue

        if result is _AMBIGUOUS:
            report.ambiguous.append(
                (
                    tx_hash,
                    destination,
                    asset_code,
                    asset_issuer,
                    amount,
                    memo,
                    created_at,
                    [],
                )
            )
            continue

        if result is _CHAIN_ORPHAN:
            # VoteSnapshot could not be resolved; fall back to legacy bribe
            # candidate lookup (±1 day) to attach a bribe_id to the orphan
            # row so operators can identify which bribe it belongs to.
            chain_date = created_at.date()
            bribe_window_start = timezone.make_aware(
                datetime.combine(chain_date - timedelta(days=1), time.min)
            )
            bribe_window_end = timezone.make_aware(
                datetime.combine(chain_date + timedelta(days=1), time.min)
            )
            fallback_candidates = list(
                AggregatedByAssetBribe.objects.filter(
                    start_at__lt=bribe_window_end,
                    stop_at__gt=bribe_window_start,
                    asset_code=asset_code,
                    asset_issuer=asset_issuer or "",
                ).select_related("market_key")
            )
            fallback_candidates = [
                b for b in fallback_candidates
                if b.market_key and b.market_key.short_value == short_value
            ]
            fallback_bribe_id = fallback_candidates[0].id if len(fallback_candidates) == 1 else None
            report.chain_only.append(
                (
                    tx_hash,
                    destination,
                    asset_code,
                    asset_issuer,
                    amount,
                    fallback_bribe_id,
                    None,
                    memo,
                    created_at,
                    None,
                )
            )
            continue

        bribe, vote_snapshot = result
        matched_chain_votesnapshots_by_bribe_date[
            (bribe.id, vote_snapshot.snapshot_time)
        ].add(vote_snapshot.id)
        report.chain_only.append(
            (
                tx_hash,
                destination,
                asset_code,
                asset_issuer,
                amount,
                bribe.id,
                vote_snapshot.id,
                memo,
                created_at,
                None,
            )
        )

    # Iterate deterministically so that if N Payouts share a 5-tuple but
    # only M < N chain ops exist for it, the (N - M) Payouts flagged as
    # db_only are stable across runs (oldest-first).
    for payout in db_success_qs.order_by("id"):
        op_tuple = (
            payout.stellar_transaction_id,
            payout.vote_snapshot.voting_account,
            payout.asset_code,
            payout.asset_issuer or "",
            payout.reward_amount,
        )
        if op_tuple in ambiguous_op_tuples:
            # The chain op matched >1 Payout and we cannot tell which one
            # was really paid. Surface rather than risk downgrading the
            # real SUCCESS row.
            report.ambiguous.append(
                (
                    payout.stellar_transaction_id,
                    payout.vote_snapshot.voting_account,
                    payout.asset_code,
                    payout.asset_issuer or "",
                    payout.reward_amount,
                    "",
                    None,
                    [payout.bribe_id],
                )
            )
            continue
        if seen_chain_op_tuples[op_tuple] > 0:
            seen_chain_op_tuples[op_tuple] -= 1
            continue
        report.db_only.append(
            (
                payout.id,
                payout.stellar_transaction_id,
                payout.vote_snapshot.voting_account,
                payout.asset_code,
                payout.asset_issuer,
                payout.reward_amount,
                payout.bribe_id,
                payout.vote_snapshot.snapshot_time,
            )
        )

    per_bribe_missed = defaultdict(dict)
    asset_holder_cache = {}
    current = from_date
    while current <= to_date:
        day_start = timezone.make_aware(datetime.combine(current, time.min))
        day_end = timezone.make_aware(datetime.combine(current, time.max))
        active_bribes = AggregatedByAssetBribe.objects.filter(
            start_at__lte=day_end, stop_at__gt=day_start
        )

        for bribe in active_bribes:
            votes_qs, total_votes_pre_dust = get_payable_votes(
                bribe,
                current,
                reward_amount=bribe.daily_amount,
                asset_holder_cache=asset_holder_cache,
            )
            if not total_votes_pre_dust:
                continue

            # Hoist the per-vote Payout existence check out of the inner
            # loop — one query per (bribe, day) instead of N_votes queries.
            existing_payout_vote_ids = set(
                Payout.objects.filter(
                    bribe=bribe,
                    vote_snapshot__snapshot_time=current,
                ).values_list("vote_snapshot_id", flat=True)
            )

            missed_count = 0
            for vote in votes_qs:
                if vote.id in existing_payout_vote_ids:
                    continue
                if (
                    vote.id
                    in matched_chain_votesnapshots_by_bribe_date[(bribe.id, current)]
                ):
                    continue

                expected_reward_amount = (
                    bribe.daily_amount * vote.votes_value / total_votes_pre_dust
                ).quantize(Decimal("0.0000000"), rounding=ROUND_DOWN)
                report.missed.append(
                    (
                        bribe.id,
                        vote.id,
                        vote.snapshot_time,
                        vote.voting_account,
                        bribe.asset_code,
                        bribe.asset_issuer,
                        expected_reward_amount,
                    )
                )
                missed_count += 1

            if missed_count:
                per_bribe_missed[bribe.id][current] = missed_count

        current += timedelta(days=1)

    report.per_bribe_missed = {
        bribe_id: dict(day_counts) for bribe_id, day_counts in per_bribe_missed.items()
    }
    return report


def apply_fix_db(
    report: ReconcileReport,
    now=None,
    cache_lock_key=None,
    owner_token=None,
    touch_every=50,
) -> dict:
    """
    Apply fix-db mutations from a reconcile report.

    When cache_lock_key and owner_token are both provided, the lock is
    refreshed (cache.touch) and re-verified every touch_every rows across
    all mutation loops. If the key has been evicted or taken over by another
    operator, FixDbLockLost is raised immediately to prevent racing.

    When cache_lock_key/owner_token are not provided (e.g. in tests that
    build a bare ReconcileReport), the touch/check path is skipped entirely.
    """
    now = now or timezone.now()
    reconciled_message = "reconciled {}".format(now.date())
    counts = {
        "chain_only_created": 0,
        "chain_only_upgraded": 0,
        "db_only_downgraded": 0,
        "ambiguous_skipped": 0,
        "chain_orphan_skipped": 0,
        "missed_skipped": 0,
    }
    _use_lock = cache_lock_key is not None and owner_token is not None
    _rows_processed = 0

    def _maybe_touch_lock():
        nonlocal _rows_processed
        if not _use_lock:
            return
        _rows_processed += 1
        if _rows_processed % touch_every == 0:
            cache.touch(cache_lock_key, PAY_REWARDS_FIX_DB_TTL)
            current_token = cache.get(cache_lock_key)
            if current_token != owner_token:
                raise FixDbLockLost(
                    "fix-db lock lost mid-run: owner token mismatch or key evicted; "
                    "aborting to avoid racing with another operator"
                )

    for item in report.chain_only:
        (
            tx_hash,
            destination,
            asset_code,
            asset_issuer,
            amount,
            bribe_id,
            vote_snapshot_id,
            _memo,
            _created_at,
            failed_payout_id,
        ) = item

        _maybe_touch_lock()

        if vote_snapshot_id is None:
            counts["chain_orphan_skipped"] += 1
            continue

        if failed_payout_id is not None:
            counts["chain_only_upgraded"] += Payout.objects.filter(
                id=failed_payout_id,
                status=Payout.STATUS_FAILED,
            ).update(
                status=Payout.STATUS_SUCCESS,
                message=reconciled_message,
            )
            continue

        with transaction.atomic():
            _payout, created = Payout.objects.get_or_create(
                stellar_transaction_id=tx_hash,
                vote_snapshot_id=vote_snapshot_id,
                bribe_id=bribe_id,
                asset_code=asset_code,
                asset_issuer=asset_issuer or "",
                reward_amount=amount,
                defaults={
                    "status": Payout.STATUS_SUCCESS,
                    "message": reconciled_message,
                },
            )
            if created:
                counts["chain_only_created"] += 1

    for (
        payout_id,
        _tx_hash,
        _voting_account,
        _asset_code,
        _asset_issuer,
        _amount,
        _bribe_id,
        _snapshot_time,
    ) in report.db_only:
        _maybe_touch_lock()
        counts["db_only_downgraded"] += Payout.objects.filter(
            id=payout_id,
            status=Payout.STATUS_SUCCESS,
        ).update(
            status=Payout.STATUS_FAILED,
            message="db_only — no chain tx (reconciled {})".format(now.date()),
        )

    counts["ambiguous_skipped"] = len(report.ambiguous)
    counts["missed_skipped"] = len(report.missed)
    return counts
