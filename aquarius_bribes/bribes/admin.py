from django.contrib import admin

from aquarius_bribes.bribes.models import AggregatedByAssetBribe, Bribe


@admin.register(Bribe)
class BribeAdmin(admin.ModelAdmin):
    list_display = [
        '__str__', 'status', 'short_asset', 'get_short_market_key', 'unlock_time', 'created_at', 'loaded_at', 'updated_at',
    ]
    list_filter = ['status',]
    readonly_fields = [
        'status', 'message', 'market_key_id', 'sponsor', 'amount', 'asset_code',
        'asset_issuer', 'amount_for_bribes', 'amount_aqua', 'convertation_tx_hash',
        'claimable_balance_id', 'paging_token', 'unlock_time', 'start_at',
        'stop_at', 'created_at', 'loaded_at', 'updated_at',
    ]
    search_fields = ['market_key__market_key', 'claimable_balance_id', 'sponsor',]
    ordering = ['-loaded_at',]

    def get_short_market_key(self, obj):
        return '{}...{}'.format(obj.market_key_id[:4], obj.market_key_id[-4:])


@admin.register(AggregatedByAssetBribe)
class AggregatedByAssetBribeAdmin(admin.ModelAdmin):
    list_display = [
        '__str__', 'asset_code', 'asset_issuer', 'market_key_id',
        'daily_amount', 'total_reward_amount', 'start_at', 'stop_at', 'created_at',
    ]
    readonly_fields = [
        'asset_code', 'asset_issuer', 'market_key_id', 'start_at', 'stop_at', 'created_at', 'updated_at',
    ]
    search_fields = ['market_key__market_key',]
    ordering = ['-created_at',]
