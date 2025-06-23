from django_filters import FilterSet

from aquarius_bribes.bribes.models import Bribe


class BribeFilter(FilterSet):
    class Meta:
        model = Bribe
        fields = {
            'start_at': ['lt', 'gt', 'gte', 'lte'],
            'stop_at': ['lt', 'gt', 'gte', 'lte'],
            'asset_code': ['exact', ],
            'asset_issuer': ['exact', 'isnull'],
            'aqua_total_reward_amount_equivalent': ['lt', 'gt', 'gte', 'lte'],
            'market_key': ['exact', ],
            'is_amm_protocol': ['exact',]
        }
