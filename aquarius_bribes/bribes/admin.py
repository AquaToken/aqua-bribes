from django.contrib import admin

from aquarius_bribes.bribes.models import Bribe


@admin.register(Bribe)
class BribeAdmin(admin.ModelAdmin):
    list_display = [
        '__str__', 'status', 'short_asset', 'get_short_market_key', 'created_at', 'loaded_at', 'updated_at',
    ]
    readonly_fields = [
        'status', 'message', 'market_key', 'sponsor', 'amount', 'asset_code',
        'asset_issuer', 'amount_for_bribes', 'amount_aqua', 'convertation_tx_hash',
        'claimable_balance_id', 'paging_token', 'unlock_time', 'start_at',
        'stop_at', 'created_at', 'loaded_at', 'updated_at',
    ]
    search_fields = ['market_key', 'claimable_balance_id', 'sponsor',]
    ordering = ['-loaded_at',]

    def get_short_market_key(self, obj):
        return '{}...{}'.format(obj.market_key[:4], obj.market_key[-4:])
