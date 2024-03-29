from django.db import models


class VoteSnapshot(models.Model):
    market_key = models.ForeignKey('bribes.MarketKey', null=True, on_delete=models.PROTECT)

    votes_value = models.DecimalField(max_digits=20, decimal_places=7)
    voting_account = models.CharField(max_length=56, db_index=True)

    snapshot_time = models.DateField(db_index=True)

    def __str__(self):
        return 'VoteSnapshot: {}..{} ({})'.format(
            self.voting_account[:4], self.voting_account[-4:], self.snapshot_time,
        )

    class Meta:
        unique_together = ('snapshot_time', 'market_key', 'voting_account')


class Payout(models.Model):
    STATUS_SUCCESS = 'success'
    STATUS_FAILED = 'failed'
    STATUS_CHOICES = (
        (STATUS_SUCCESS, 'success'),
        (STATUS_FAILED, 'failed'),
    )

    bribe = models.ForeignKey('bribes.AggregatedByAssetBribe', on_delete=models.PROTECT)

    vote_snapshot = models.ForeignKey(VoteSnapshot, on_delete=models.PROTECT)

    stellar_transaction_id = models.CharField(max_length=64, blank=True)
    status = models.CharField(
        choices=STATUS_CHOICES, default=STATUS_SUCCESS, max_length=30, db_index=True,
    )
    message = models.TextField(blank=True, db_index=True)

    reward_amount = models.DecimalField(max_digits=20, decimal_places=7, null=True)

    asset_code = models.CharField(max_length=12)
    asset_issuer = models.CharField(max_length=56)

    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return 'Payout {0} for {1}'.format(
            self.reward_amount,
            self.vote_snapshot.voting_account,
        )


class AssetHolderBalanceSnapshot(models.Model):
    account = models.CharField(max_length=255, db_index=True)

    asset_code = models.CharField(max_length=12)
    asset_issuer = models.CharField(max_length=56)

    balance = models.DecimalField(max_digits=20, decimal_places=7)

    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    def __str__(self):
        return '{0}..{1} at {2}: {3} {4}'.format(
            self.account[:8], self.account[-8:], self.created_at.date(), self.balance, self.asset.code,
        )
