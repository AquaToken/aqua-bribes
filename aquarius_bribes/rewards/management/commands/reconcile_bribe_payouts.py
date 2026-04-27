import uuid
from collections import defaultdict
from datetime import datetime

from django.conf import settings
from django.core.cache import cache
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from aquarius_bribes.rewards.reconcile import FixDbLockLost, apply_fix_db, reconcile_bribe_payouts
from aquarius_bribes.rewards.tasks import (
    PAY_REWARDS_FIX_DB_ACTIVE_KEY,
    PAY_REWARDS_FIX_DB_TTL,
)


class Command(BaseCommand):
    help = "Reconcile bribe payouts; dry-run by default."

    def add_arguments(self, parser):
        parser.add_argument("--from", dest="from_date", required=True)
        parser.add_argument("--to", dest="to_date", required=True)
        parser.add_argument(
            "--fix-db",
            dest="fix_db",
            action="store_true",
            default=False,
        )

    def handle(self, *args, **options):
        from_date = self._parse_date(options["from_date"], "--from")
        to_date = self._parse_date(options["to_date"], "--to")
        if from_date > to_date:
            raise CommandError("--from must be on or before --to")

        bribe_wallet = getattr(settings, "BRIBE_WALLET_ADDRESS", "")
        if not bribe_wallet:
            raise CommandError("settings.BRIBE_WALLET_ADDRESS is not set; cannot reconcile.")

        if options["fix_db"]:
            today_utc = timezone.now().date()
            if to_date >= today_utc:
                raise CommandError(
                    "--fix-db cannot touch date {} which is today or in the future. "
                    "task_pay_rewards is still writing Payouts for today; reconcile "
                    "report would go stale between build and write. Wait until the "
                    "UTC day ends, then reconcile past dates only.".format(
                        to_date
                    )
                )

        report = reconcile_bribe_payouts(from_date, to_date, bribe_wallet)
        if options["fix_db"]:
            self._render_fix_db_plan(report)
            if input("Proceed? [y/N]: ") != "y":
                self.stdout.write("Aborted.")
                return

            owner_token = uuid.uuid4().hex
            if not cache.add(
                PAY_REWARDS_FIX_DB_ACTIVE_KEY, owner_token, PAY_REWARDS_FIX_DB_TTL
            ):
                raise CommandError(
                    "Another --fix-db run is in progress (PAY_REWARDS_FIX_DB_ACTIVE_KEY "
                    "is held). Wait for it to finish, or clear the cache key if the "
                    "previous run crashed without cleanup."
                )
            try:
                counts = apply_fix_db(
                    report,
                    now=timezone.now(),
                    cache_lock_key=PAY_REWARDS_FIX_DB_ACTIVE_KEY,
                    owner_token=owner_token,
                )
                self.stdout.write("Fix-db applied: {}".format(counts))
            except FixDbLockLost as exc:
                self.stderr.write("Fix-db aborted: {}".format(str(exc)))
                raise CommandError(str(exc)) from exc
            finally:
                # Only release the lock if we still own it — a stale TTL-expiry
                # could have let a sibling run acquire it under our feet.
                if cache.get(PAY_REWARDS_FIX_DB_ACTIVE_KEY) == owner_token:
                    cache.delete(PAY_REWARDS_FIX_DB_ACTIVE_KEY)
            return

        self._render_text(report)

    def _parse_date(self, value, option_name):
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError as exc:
            raise CommandError(
                "{} must be in YYYY-MM-DD format".format(option_name)
            ) from exc

    def _render_text(self, report):
        lines = [
            "Reconcile bribe wallet {} for {}..{}".format(
                report.bribe_wallet,
                report.from_date,
                report.to_date,
            ),
            "",
            "Chain payments: {} (bribe-memo only)".format(report.chain_payments_count),
            "DB Payout success: {}".format(report.db_success_count),
            "DB Payout failed: {}".format(report.db_failed_count),
            "",
            "Bucket MATCHED: {}".format(len(report.matched)),
            "Bucket CHAIN_ONLY: {}".format(len(report.chain_only)),
            "Bucket DB_ONLY: {}".format(len(report.db_only)),
            "Bucket AMBIGUOUS: {}".format(len(report.ambiguous)),
            "Bucket MISSED: {}".format(len(report.missed)),
            "Skipped (chain op past --to date): {}".format(
                report.skipped_future_window_chain_op,
            ),
            "",
        ]

        missed_by_date = defaultdict(list)
        for bribe_id, day_dict in sorted(report.per_bribe_missed.items()):
            for missed_date, count in sorted(day_dict.items()):
                if count > 0:
                    missed_by_date[missed_date].append((bribe_id, count))

        if missed_by_date:
            for missed_date in sorted(missed_by_date):
                lines.append("Per-bribe missed on {}:".format(missed_date))
                for bribe_id, count in missed_by_date[missed_date]:
                    lines.append("  bribe_id={}: missed={}".format(bribe_id, count))
        else:
            lines.extend(["Per-bribe missed:", "  (empty)"])

        lines.append("")
        lines.append("Per-bribe ambiguous:")
        if report.ambiguous:
            for row in report.ambiguous:
                lines.append(
                    "  tx_hash={}: candidate_bribe_ids={}".format(
                        row[0],
                        "|".join(map(str, row[7])),
                    )
                )
        else:
            lines.append("  (empty)")

        self.stdout.write("\n".join(lines))

    def _render_fix_db_plan(self, report):
        lines = [
            "Fix-db plan for {}..{}".format(report.from_date, report.to_date),
            "Bucket MATCHED: {}".format(len(report.matched)),
            "Bucket CHAIN_ONLY: {}".format(len(report.chain_only)),
            "Bucket DB_ONLY: {}".format(len(report.db_only)),
            "Bucket AMBIGUOUS: {}".format(len(report.ambiguous)),
            "Bucket MISSED: {}".format(len(report.missed)),
            "Bucket CHAIN_ORPHAN: {}".format(report.chain_orphan_count),
            "Skipped (chain op past --to date): {}".format(
                report.skipped_future_window_chain_op,
            ),
            "",
            "CHAIN_ONLY examples (first 10):",
        ]
        if report.chain_only:
            for row in report.chain_only[:10]:
                action = "create"
                if row[6] is None:
                    action = "skip-chain-orphan"
                elif row[9] is not None:
                    action = "upgrade"
                lines.append(
                    "  {} tx_hash={} destination={} asset={}:{} amount={} bribe_id={} vote_snapshot_id={} failed_payout_id={}".format(
                        action,
                        row[0],
                        row[1],
                        row[2],
                        row[3] or "",
                        row[4],
                        row[5],
                        row[6],
                        row[9],
                    )
                )
        else:
            lines.append("  (empty)")

        lines.append("")
        lines.append("DB_ONLY examples (first 10):")
        if report.db_only:
            for row in report.db_only[:10]:
                lines.append(
                    "  downgrade payout_id={} tx_hash={} destination={} asset={}:{} amount={} bribe_id={} snapshot_time={}".format(
                        row[0],
                        row[1],
                        row[2],
                        row[3],
                        row[4] or "",
                        row[5],
                        row[6],
                        row[7],
                    )
                )
        else:
            lines.append("  (empty)")

        self.stdout.write("\n".join(lines))
