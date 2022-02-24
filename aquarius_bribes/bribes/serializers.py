from rest_framework import serializers

from aquarius_bribes.bribes.models import AggregatedByAssetBribe, MarketKey


class AggregatedByAssetBribeSerializer(serializers.ModelSerializer):
    class Meta:
        model = AggregatedByAssetBribe
        fields = (
            'market_key', 'total_reward_amount', 'start_at',
            'stop_at', 'asset_code', 'asset_issuer', 'daily_amount',
        )


class MarketKeySerializer(serializers.ModelSerializer):
    aggregated_bribes = AggregatedByAssetBribeSerializer(many=True)

    class Meta:
        model = MarketKey
        fields = ('market_key', 'aggregated_bribes')