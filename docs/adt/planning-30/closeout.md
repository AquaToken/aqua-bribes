# Planning #30 closeout

## Outcome

Replay-safe bribe ingestion is implemented and published as a ready pull
request: [AquariusDeFi/aqua-bribes#7](https://github.com/AquariusDeFi/aqua-bribes/pull/7).

- Base: `master` at `0c3bf061f0743a0421b4319390ff76a73ab6ca96`.
- Verified code commit: `1f9effbf45abb7586a71e8672ea93260c2845ad1`.
- Branch: `codex/planning-30-replay-safe-ingestion-minimal`.
- Source card: [AquariusDeFi/planning#30](https://github.com/AquariusDeFi/planning/issues/30).

## Delivered behavior

- A durable cursor records the raw Horizon page tail per collector account.
- Wrong claimant counts are permanently skipped only after core source
  metadata is valid; their warning is emitted only by the worker that commits
  the page.
- MarketKeys, new Bribes, and cursor movement share one transaction.
- Stale overlapping workers cannot regress the cursor or persist stale page
  data, and replay does not duplicate an accepted Bribe.
- Missing or malformed core metadata and Horizon, parse, enrichment, or
  database failures remain retry-blocking.
- Strict-send selection, claim/return tasks, status semantics, and reward
  accounting are unchanged.

## Regression evidence

- Focused ingestion and PostgreSQL concurrency tier: 11/11 passed.
- Rebuilt full Django tier on `1f9effb`: 50/50 passed in 541.4 seconds,
  including Django check and migration-drift check.
- New Python files pass flake8. All changed Python files pass isort and
  `py_compile`; `git diff --check` passes.
- Existing repository-wide flake8 and runtime warnings remain baseline debt;
  no new warning was introduced on changed lines.

The pre-transaction empty-cursor write and legacy live-test collector leak were
both captured as RED before repair. The cold reviewer established the
wrong-count/malformed-metadata intersection; its new regression discriminates
the old behavior and passes on the repaired commit.

## Independent review

A counted final-cold review of the first immutable candidate `825b5aa` returned
one material `HOLD`: claimant-count skipping could consume a record whose core
source metadata was malformed. Commit `1f9effb` validates the claimant list,
record identity, paging token, and source timestamp before the skip boundary.
An informed exact-diff validation returned PASS, and the repaired full suite is
green. No second cold generation was started because no blocker remained after
the bounded repair.

This review state supports the ready PR but is not deployment or merge
authority.

## Rollout and residual risk

Migration `bribes.0010_bribeingestioncursor` is schema-only. An account without
a cursor begins at the start of available Horizon history; existing
`Bribe.claimable_balance_id` uniqueness makes accepted rows idempotent. No
production deployment was performed.

After deployment, verify forward cursor progress, continued ingestion of new
valid bribes, and disappearance of repeated expected-invalid events. Production
mixed-version operation, collector rotation, and deployment execution were not
part of this change.

## Next action

Review and merge the ready PR, then perform the documented migration rollout
and post-deploy checks.
