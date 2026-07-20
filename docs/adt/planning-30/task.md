# Planning #30: replay-safe bribe ingestion

## Outcome

Make claimable-balance ingestion advance by the raw Horizon page tail only after
the page's accepted bribes are durable. Expected non-bribe records must not
stall the cursor, while malformed source data and failed work must remain
retryable.

## Base and scope

- Repository base: `master` at
  `0c3bf061f0743a0421b4319390ff76a73ab6ca96`.
- Source issue: `AquariusDeFi/planning#30`.
- Preserve existing quote selection, bribe statuses, claim/return behavior, and
  reward accounting.

## Effective specification

1. Store source-page progress separately from accepted `Bribe` rows, keyed by
   the configured collector account.
2. Use the raw final record's paging token as the next cursor, including when a
   page contains no accepted bribes.
3. Permanently skip only records whose claimant count is not two. Emit one
   actionable warning during the successful page attempt.
4. Treat malformed or missing Horizon metadata, including
   `last_modified_time`, and all other parse, enrichment, Horizon, or database
   failures as retry-blocking. Such failures must not persist page work or move
   the cursor.
5. Call the record parser once per fetched record. Persist required market keys,
   new bribes, and the page cursor in one transaction.
6. Protect the cursor from a stale overlapping loader run. Replaying an already
   durable bribe must not create a duplicate.
7. An account without a cursor starts from the beginning. The first deployment
   may therefore replay available history; `Bribe.claimable_balance_id`
   uniqueness keeps accepted rows idempotent.

## Non-goals

- Redesigning or validating the complete Horizon response schema.
- Changing strict-send route selection or amount semantics.
- Expanding which bribe statuses are refunded.
- Supporting collector rotation or multi-account downstream ownership.
- Verifying a production deployment or performing rollout from this PR.

## Decision log

- 2026-07-19: Replaced the previous ten-repair candidate with a clean branch
  from `master`; that candidate had grown into a broad defensive-ingestion
  framework and changed unrelated financial behavior.
- 2026-07-19: Kept the already accepted classification boundary: wrong claimant
  count is skippable; parse, Horizon, enrichment, and persistence failures are
  retry-blocking. No waiver was inferred that would allow malformed Horizon
  metadata to trigger payout or refund behavior.
- 2026-07-19: Selected a small durable database cursor because an expiring cache
  or the latest accepted `Bribe` cannot remember progress past an invalid page
  tail.
- 2026-07-20: A final-cold review found that claimant-count skipping could hide
  malformed source identity. Core classification metadata (`claimants` as a
  list, non-empty `id` and `paging_token`, and parseable
  `last_modified_time`) is now validated before a permanent skip. Bribe-specific
  fields remain behind the claimant-count gate; complete Horizon schema
  validation remains out of scope.

## Verification contract

Deterministic tests must cover a mixed page with an invalid tail, an entirely
invalid page, one parse call per record, malformed timestamp retry, Horizon
failure, database rollback without cursor movement, duplicate-safe replay, and
stale-worker cursor protection. Run one rebuilt full Django test tier on the
final candidate, plus focused static checks and an independent final review.

## Verification log

- Focused RED: five timestamp cases exposed the pre-transaction empty cursor
  write; the loader was changed to create the cursor only in the page commit.
- Focused GREEN: 10 ingestion tests and 1 PostgreSQL two-connection concurrency
  test pass.
- Existing live-test isolation was reproduced as a 1/2 failure and repaired;
  the same pair then passed 2/2 with one collector wallet per test.
- Rebuilt full Django tier on pre-review commit `825b5aa`: 49/49 tests passed
  in 551.7 seconds. The repaired commit receives a fresh final gate.
- New Python files pass flake8 and all changed Python files pass isort. The
  repository's legacy files retain pre-existing flake8 and runtime warnings;
  no new warning was introduced on changed lines.
- Final-cold review of `825b5aa` returned one material `HOLD`: a wrong-count
  record could still hide malformed core metadata. The focused intersection
  regression and narrow repair are complete; final full verification and
  informed repair validation remain pending.
