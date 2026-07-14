#!/usr/bin/env bash
# Run the Django test suite only (skip flake8 / isort). Used for fast
# feedback during feature work and audit verification — CI still runs the
# full runtests.sh (lint + imports + tests).
#
# Extra args are forwarded to manage.py test, e.g.:
#   ./run_django_tests.sh aquarius_bribes.rewards.tests.ReconcileAndMonitoringTests -v 2
set -e

python -W ignore manage.py check
python -W ignore manage.py makemigrations --dry-run --check
python manage.py test --noinput --keepdb "$@"
