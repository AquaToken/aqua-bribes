"""
Local-only settings for pre-deploy validation of reconcile + monitoring commands
against an AWS RDS prod replica.

Not intended for commit. This file overrides dev.py hardcoded testnet values
(HORIZON_URL, STELLAR_PASSPHRASE, BRIBE_WALLET_ADDRESS) so they can be pointed at
pubnet + the prod bribe wallet via environment variables.

See repos/aqua-bribes/design/2026-04-21-reconcile-local-prevalidation-runbook.md
"""
from config.settings.dev import *  # noqa: F401,F403

HORIZON_URL = env('HORIZON_URL', default='https://horizon.stellar.org')
STELLAR_PASSPHRASE = env(
    'STELLAR_PASSPHRASE',
    default='Public Global Stellar Network ; September 2015',
)
BRIBE_WALLET_ADDRESS = env('BRIBE_WALLET_ADDRESS')

BRIBE_WALLET_SIGNER = env('BRIBE_WALLET_SIGNER', default=None)

PAYOUT_COMPLETENESS_ALERT_ENABLED = env.bool(
    'PAYOUT_COMPLETENESS_ALERT_ENABLED', default=False,
)
SENTRY_ENABLED = False

assert 'testnet' not in HORIZON_URL, (
    'prevalidation.py points at testnet Horizon — check HORIZON_URL env var'
)
assert 'Public Global' in STELLAR_PASSPHRASE, (
    'prevalidation.py still uses testnet passphrase — check STELLAR_PASSPHRASE env var'
)
