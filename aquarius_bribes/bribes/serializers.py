from rest_framework import serializers

from aquarius_bribes.bribes.models import AggregatedByAssetBribe, MarketKey


class AggregatedByAssetBribeSerializer(serializers.ModelSerializer):
    daily_aqua_equivalent = serializers.DecimalField(max_digits=20, decimal_places=7)
    daily_amount = serializers.DecimalField(max_digits=20, decimal_places=7)

    class Meta:
        model = AggregatedByAssetBribe
        fields = (
            'market_key', 'total_reward_amount', 'start_at',
            'stop_at', 'asset_code', 'asset_issuer', 'daily_amount',
            'aqua_total_reward_amount_equivalent', 'daily_aqua_equivalent',
        )


class MarketKeySerializer(serializers.ModelSerializer):
    aggregated_bribes = AggregatedByAssetBribeSerializer(many=True)

    class Meta:
        model = MarketKey
        fields = ('market_key', 'aggregated_bribes')