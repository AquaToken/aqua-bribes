from django.db import migrations, models


class Migration(migrations.Migration):
    """
    Composite index on AssetHolderBalanceSnapshot(asset_code, asset_issuer,
    created_at) — used by get_payable_votes() and therefore by task_pay_rewards,
    reconcile_bribe_payouts, and check_payout_completeness.

    Before this index the planner used the single-column created_at index and
    filtered asset_code/asset_issuer via seq scan over the day's rows.

    CREATE INDEX CONCURRENTLY to avoid locking the table on prod; atomic=False
    is required because CONCURRENTLY cannot run inside a transaction.
    """
    atomic = False

    dependencies = [
        ('rewards', '0012_claimablebalance_claimant'),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[
                migrations.RunSQL(
                    sql=(
                        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
                        '"ahbs_asset_date_idx" '
                        'ON "rewards_assetholderbalancesnapshot" '
                        '("asset_code", "asset_issuer", "created_at");'
                    ),
                    reverse_sql=(
                        'DROP INDEX CONCURRENTLY IF EXISTS "ahbs_asset_date_idx";'
                    ),
                ),
            ],
            state_operations=[
                migrations.AddIndex(
                    model_name='assetholderbalancesnapshot',
                    index=models.Index(
                        fields=['asset_code', 'asset_issuer', 'created_at'],
                        name='ahbs_asset_date_idx',
                    ),
                ),
            ],
        ),
    ]
