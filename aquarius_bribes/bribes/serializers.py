from rest_framework import serializers

from aquarius_bribes.bribes.models import Bribe


class BribeSerializer(serializers.ModelSerializer):
    class Meta:
        model = Bribe
        fields = (
            'market_key', 'sponsor', 'amount', 'amount_for_bribes', 'amount_aqua',
            'claimable_balance_id', 'unlock_time', 'start_at', 'stop_at', 'created_at',
        )
