from datetime import datetime, time, timedelta, timezone as datetime_timezone
from decimal import Decimal

import sentry_sdk

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from aquarius_bribes.bribes.models import AggregatedByAssetBribe
from aquarius_bribes.rewards.eligibility import get_payable_votes
from aquarius_bribes.rewards.models import Payout


def _emit_alert(message, level, snapshot_date, bribes):
    sentry_sdk.capture_message(
        message,
        level=level,
        contexts={
            "bribe_completeness": {
                "date": snapshot_date.isoformat(),
                "bribes": bribes,
            },
        },
    )


def run_completeness_check(date, threshold_pct, emit_alert):
    date_start = datetime.combine(date, time.min).replace(tzinfo=datetime_timezone.utc)
    date_end = datetime.combine(date, time.max).replace(tzinfo=datetime_timezone.utc)
    results = []
    has_critical = False
    has_warning = False
    warning_threshold = Decimal("1") - (Decimal(threshold_pct) / Decimal("100"))

    active_bribes = AggregatedByAssetBribe.objects.filter(
        start_at__lte=date_end,
        stop_at__gt=date_start,
    ).select_related("market_key")

    asset_holder_cache = {}

    for bribe in active_bribes:
        payable, _total_votes = get_payable_votes(
            bribe,
            date,
            reward_amount=bribe.daily_amount,
            asset_holder_cache=asset_holder_cache,
        )
        payable_count = payable.count()
        if payable_count == 0:
            continue

        paid_count = (
            Payout.objects.filter(
                bribe=bribe,
                vote_snapshot__snapshot_time=date,
                vote_snapshot__in=payable,
                status=Payout.STATUS_SUCCESS,
            )
            .values("vote_snapshot")
            .distinct()
            .count()
        )
        missing_count = payable_count - paid_count

        if paid_count == 0 and payable_count > 0:
            severity = "CRITICAL"
            has_critical = True
        elif Decimal(paid_count) < (Decimal(payable_count) * warning_threshold):
            severity = "WARNING"
            has_warning = True
        else:
            severity = "OK"

        results.append(
            {
                "bribe_id": bribe.id,
                "asset_code": bribe.asset_code,
                "market_key": bribe.market_key_id or "",
                "payable": payable_count,
                "paid": paid_count,
                "missing": missing_count,
                "severity": severity,
            }
        )

    if emit_alert and has_critical:
        _emit_alert(
            "bribe-payout-completeness: CRITICAL",
            "error",
            date,
            [result for result in results if result["severity"] == "CRITICAL"],
        )
    elif emit_alert and has_warning:
        _emit_alert(
            "bribe-payout-completeness: WARNING",
            "warning",
            date,
            [result for result in results if result["severity"] == "WARNING"],
        )

    return {
        "results": results,
        "has_critical": has_critical,
        "has_warning": has_warning,
    }


class Command(BaseCommand):
    help = "Check bribe payout completeness for a UTC date."

    def add_arguments(self, parser):
        parser.add_argument(
            "--date",
            dest="date",
            metavar="YYYY-MM-DD",
            help="UTC date to inspect. Defaults to yesterday UTC.",
        )

    def handle(self, *args, **options):
        snapshot_date = options["date"]
        if snapshot_date is None:
            snapshot_date = timezone.now().date() - timedelta(days=1)
        else:
            snapshot_date = self._parse_date(snapshot_date)

        # CLI is for ad-hoc human debugging — never emits Sentry alerts.
        # Production alerting is driven solely by task_check_payout_completeness
        # which reads settings.PAYOUT_COMPLETENESS_ALERT_ENABLED.
        report = run_completeness_check(
            date=snapshot_date,
            threshold_pct=settings.PAYOUT_COMPLETENESS_THRESHOLD_PCT,
            emit_alert=False,
        )
        self.stdout.write(self._render_table(report["results"]))

        if report["has_critical"]:
            raise SystemExit(1)

    def _parse_date(self, value):
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError as exc:
            raise CommandError("--date must be in YYYY-MM-DD format") from exc

    def _render_table(self, results):
        lines = [
            "{:<8} {:<12} {:<20} {:>7} {:>7} {:>7} {:<8}".format(
                "bribe_id",
                "asset_code",
                "market_key",
                "payable",
                "paid",
                "missing",
                "severity",
            )
        ]

        for result in results:
            lines.append(
                "{:<8} {:<12} {:<20} {:>7} {:>7} {:>7} {:<8}".format(
                    result["bribe_id"],
                    result["asset_code"],
                    (result["market_key"] or "")[:20],
                    result["payable"],
                    result["paid"],
                    result["missing"],
                    result["severity"],
                )
            )

        return "\n".join(lines)
