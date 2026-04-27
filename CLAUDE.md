# aqua-bribes — Claude instructions

Django backend for Aquarius bribe payouts. Tests run inside Docker; do not
try to run the Django test suite against the live DB (the configured
hostname resolves to RDS and is unreachable from dev machines).

## Running tests

Start Postgres once per session, then use `scripts/run_django_tests.sh`:

```sh
docker compose -f docker/docker-compose.test.yml -p aquabribes_test up -d db

# whole reconcile/rewards suite
docker compose -f docker/docker-compose.test.yml -p aquabribes_test run --rm backend \
  bash -c "/code/scripts/wait-for-it.sh db:5432 -t 60 && /code/scripts/run_django_tests.sh \
  aquarius_bribes.rewards.tests.ReconcileAndMonitoringTests -v 2"

# narrower — pass any manage.py test label(s)
docker compose -f docker/docker-compose.test.yml -p aquabribes_test run --rm backend \
  bash -c "/code/scripts/wait-for-it.sh db:5432 -t 60 && /code/scripts/run_django_tests.sh \
  aquarius_bribes.rewards.tests.ReconcileAndMonitoringTests.test_reconcile_buckets_matched -v 2"

# teardown when the session is done
docker compose -f docker/docker-compose.test.yml -p aquabribes_test down -v
```

`run_django_tests.sh` wraps `manage.py check` + `makemigrations --dry-run --check`
+ `manage.py test --noinput --keepdb "$@"` — lint and isort are NOT run.
CI still runs the full `scripts/runtests.sh` (lint + imports + tests), so do
not rely on this for style checks.

`--keepdb` persists the test database across runs, so the second invocation
skips schema setup.

See `TESTING.md` at repo root for the full runbook (CI-parity flow,
troubleshooting notes).

## Do not

- Do not run `manage.py test` against `config.settings.prod` / `staging` —
  those point at RDS and the test runner will drop/recreate the DB.
- Do not skip the docker-compose teardown (`down -v`) when switching to a
  different project; leftover volumes hold stale migrations.
- Do not commit `.DS_Store`, `.idea/`, `__pycache__/`, or anything under
  `.ruff_cache/`. They are gitignored but appear as untracked in noisy
  `git status` output.
