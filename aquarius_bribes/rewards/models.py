from django.db import models


class VoteSnapshot(models.Model):
    market_key = models.CharField(max_length=56)

    votes_value = models.DecimalField(max_digits=20, decimal_places=7)
    voting_account = models.CharField(max_length=56)

    snapshot_time = models.DateTimeField()

    def __str__(self):
        return 'VoteSnapshot: {}..{} ({})'.format(
            self.voting_account[:4], self.voting_account[-4:], self.snapshot_time,
        )


class Payout(models.Model):
    STATUS_SUCCESS = 'success'
    STATUS_FAILED = 'failed'
    STATUS_CHOICES = (
        (STATUS_SUCCESS, 'success'),
        (STATUS_FAILED, 'failed'),
    )

    bribe = models.ForeignKey('bribes.Bribe', on_delete=models.PROTECT)

    vote_snapshot = models.ForeignKey(VoteSnapshot, on_delete=models.PROTECT)

    stellar_transaction_id = models.CharField(max_length=64, blank=True)
    status = models.CharField(
        choices=STATUS_CHOICES, default=STATUS_SUCCESS, max_length=30,
    )
    message = models.TextField(blank=True)

    reward_amount = models.DecimalField(max_digits=20, decimal_places=7, null=True)

    asset_code = models.CharField(max_length=12)
    asset_issuer = models.CharField(max_length=56)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return 'Payout {0} for {1}'.format(
            self.reward_amount,
            self.vote_snapshot.voting_account,
        )