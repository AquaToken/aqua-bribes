from datetime import timedelta
from decimal import ROUND_DOWN, Decimal

from django.db import models

from stellar_sdk import Asset


class MarketKey(models.Model):
    market_key = models.CharField(max_length=56, primary_key=True)

    def __str__(self):
        return self.market_key


class Bribe(models.Model):
    DEFAULT_DURATION = timedelta(days=7)

    STATUS_PENDING = 0
    STATUS_INVALID = 1
    STATUS_ACTIVE = 2
    STATUS_RETURNED = 3
    STATUS_PENDING_RETURN = 4
    STATUS_FAILED_CLAIM = 5
    STATUS_NO_PATH_FOR_CONVERSION = 6
    STATUS_FAILED_RETURN = 7
    STATUS_FINISHED = 8

    STATUS_CHOICES = (
        (STATUS_PENDING, 'Pending unlock time'),
        (STATUS_INVALID, 'Invalid bribe'),
        (STATUS_ACTIVE, 'Active bribe'),
        (STATUS_RETURNED, 'Returned'),
        (STATUS_PENDING_RETURN, 'Pending unlock time to return'),
        (STATUS_FAILED_CLAIM, 'Failed claim'),
        (STATUS_NO_PATH_FOR_CONVERSION, 'Conversion failed'),
        (STATUS_FAILED_RETURN, 'Failed return'),
        (STATUS_FINISHED, 'Finished'),
    )

    status = models.IntegerField(choices=STATUS_CHOICES)
    message = models.TextField()

    market_key = models.ForeignKey(MarketKey, related_name='bribes', null=True, on_delete=models.PROTECT)

    sponsor = models.CharField(max_length=56)
    amount = models.DecimalField(max_digits=20, decimal_places=7)

    is_amm_protocol = models.BooleanField(default=False, help_text="AMM Protocol bribes are created using a "
                                                                   "fraction of collected protocol fees")

    aqua_total_reward_amount_equivalent = models.DecimalField(max_digits=20, decimal_places=7, null=True)

    asset_code = models.CharField(max_length=12)
    asset_issuer = models.CharField(max_length=56)

    amount_for_bribes = models.DecimalField(max_digits=20, decimal_places=7, null=True)
    amount_aqua = models.DecimalField(max_digits=20, decimal_places=7, null=True)
    convertation_tx_hash = models.CharField(max_length=255, null=True, default=None)
    refund_tx_hash = models.CharField(max_length=255, null=True, default=None)

    claimable_balance_id = models.CharField(max_length=255, unique=True)
    paging_token = models.CharField(max_length=255)

    unlock_time = models.DateTimeField(null=True)

    start_at = models.DateTimeField(null=True)
    stop_at = models.DateTimeField(null=True)

    created_at = models.DateTimeField(null=True)

    loaded_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return "Bribe: {0}...{1} {2}...{3}".format(
            self.market_key_id[:4], self.market_key_id[-4:],
            self.claimable_balance_id[:4], self.claimable_balance_id[-4:],
        )

    def update_active_period(self, time=None, duration=DEFAULT_DURATION):
        if time is None:
            time = self.unlock_time

        if time is None:
            return

        start_at = time + timedelta(days=8 - time.isoweekday())
        self.start_at = start_at.replace(hour=0, minute=0, second=0, microsecond=0)
        self.stop_at = self.start_at + duration

    @property
    def short_asset(self):
        asset = self.asset_code
        if self.asset_issuer:
            asset += ':{}...{}'.format(self.asset_issuer[:4], self.asset_issuer[-4:])
        return asset

    @property
    def asset(self):
        if self.asset_code == Asset.native().code and self.asset_issuer == '':
            return Asset.native()
        else:
            return Asset(code=self.asset_code, issuer=self.asset_issuer)

    @property
    def daily_bribe_amount(self):
        return Decimal(self.amount_for_bribes / self.DEFAULT_DURATION.days).quantize(
            Decimal('0.0000001'), rounding=ROUND_DOWN,
        )

    @property
    def daily_aqua_amount(self):
        return Decimal(self.amount_aqua / self.DEFAULT_DURATION.days).quantize(
            Decimal('0.0000001'), rounding=ROUND_DOWN,
        )


class AggregatedByAssetBribe(models.Model):
    market_key = models.ForeignKey(MarketKey, related_name='aggregated_bribes', null=True, on_delete=models.PROTECT)

    asset_code = models.CharField(max_length=12)
    asset_issuer = models.CharField(max_length=56)

    start_at = models.DateTimeField(null=True)
    stop_at = models.DateTimeField(null=True)

    total_reward_amount = models.DecimalField(max_digits=20, decimal_places=7, null=True)

    aqua_total_reward_amount_equivalent = models.DecimalField(max_digits=20, decimal_places=7, null=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        if self.asset.type == Asset.native().type:
            return self.asset.code
        return '{} ({}...{})'.format(
            self.asset.code, self.asset_issuer[:4], self.asset_issuer[-4:],
        )

    @property
    def daily_amount(self):
        return Decimal(self.total_reward_amount / Bribe.DEFAULT_DURATION.days).quantize(
            Decimal('0.0000001'), rounding=ROUND_DOWN,
        )

    @property
    def daily_aqua_equivalent(self):
        if not self.aqua_total_reward_amount_equivalent:
            return Decimal(0)

        return Decimal(self.aqua_total_reward_amount_equivalent / Bribe.DEFAULT_DURATION.days).quantize(
            Decimal('0.0000001'), rounding=ROUND_DOWN,
        )

    @property
    def asset(self):
        if self.asset_code == Asset.native().code and self.asset_issuer == '':
            return Asset.native()
        else:
            return Asset(code=self.asset_code, issuer=self.asset_issuer)
