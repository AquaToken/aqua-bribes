from django.contrib import admin

from aquarius_bribes.rewards.models import Payout, VoteSnapshot


@admin.register(VoteSnapshot)
class VoteSnapshotAdmin(admin.ModelAdmin):
    list_display = ('get_short_market_key', 'voting_account', 'votes_value', 'snapshot_time')

    def get_short_market_key(self, obj):
        return '{}...{}'.format(obj.market_key[:8], obj.market_key[-8:])
    get_short_market_key.short_description = 'Market key'


@admin.register(Payout)
class PayoutAdmin(admin.ModelAdmin):
    list_display = ('vote_snapshot', 'status', 'created_at', 'message', 'stellar_transaction_id')
    list_filter = ('created_at', 'status')
    search_fields = ('stellar_transaction_id', 'vote_snapshot__voting_account')
