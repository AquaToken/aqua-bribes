from django.contrib import admin

from aquarius_bribes.bribes.models import AggregatedByAssetBribe
from aquarius_bribes.rewards.models import AssetHolderBalanceSnapshot, Payout, VoteSnapshot


class AssetListFilter(admin.SimpleListFilter):
    title = 'asset'
    parameter_name = 'asset_code'
    related_filter_parameter = 'asset_code'

    def lookups(self, request, model_admin):
        return AggregatedByAssetBribe.objects.all().values_list('asset_code', 'asset_code').distinct()

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(asset_code=self.value())
        return queryset


@admin.register(VoteSnapshot)
class VoteSnapshotAdmin(admin.ModelAdmin):
    list_display = (
        'get_short_market_key', 'voting_account', 'votes_value', 'is_delegated', 'has_delegation', 'snapshot_time',
    )
    search_fields = ('market_key__market_key', 'voting_account')
    list_filter = ('snapshot_time', )

    def get_short_market_key(self, obj):
        return '{}...{}'.format(obj.market_key_id[:8], obj.market_key_id[-8:])
    get_short_market_key.short_description = 'Market key'


@admin.register(Payout)
class PayoutAdmin(admin.ModelAdmin):
    list_display = (
        'vote_snapshot', 'get_short_market_key', 'status', 'created_at', 'message', 'stellar_transaction_id',
    )
    list_filter = ('created_at', 'status')
    search_fields = ('stellar_transaction_id', 'vote_snapshot__voting_account', 'bribe__market_key__market_key')

    def get_short_market_key(self, obj):
        return '{}...{}'.format(obj.bribe.market_key_id[:8], obj.bribe.market_key_id[-8:])
    get_short_market_key.short_description = 'Market key'


@admin.register(AssetHolderBalanceSnapshot)
class AssetHolderBalanceSnapshotAdmin(admin.ModelAdmin):
    list_display = ('asset_code', 'get_asset_issuer', 'account', 'balance', 'created_at')
    list_filter = (AssetListFilter, 'created_at')
    search_fields = ('account', 'asset_code', 'asset_issuer')

    def get_asset_issuer(self, obj):
        return '{}...{}'.format(obj.asset_issuer[:4], obj.asset_issuer[-4:])
    get_asset_issuer.short_description = 'Asset issuer'
    get_asset_issuer.admin_order_field = '-asset_issuer'
