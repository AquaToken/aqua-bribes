from rest_framework import serializers

from aquarius_bribes.bribes.models import AggregatedByAssetBribe


class AggregatedByAssetBribeSerializer(serializers.ModelSerializer):
    class Meta:
        model = AggregatedByAssetBribe
        fields = (
            'market_key', 'amount', 'start_at', 'stop_at', 'asset_code', 'asset_issuer', 'daily_amount',
        )
