from django_filters import FilterSet

from aquarius_bribes.bribes.models import Bribe


class BribeFilter(FilterSet):
    class Meta:
        model = Bribe
        fields = {
            'start_at': ['lt', 'gt'],
            'stop_at': ['lt', 'gt'],
            'asset_code': ['exact', ],
            'asset_issuer': ['exact', 'isnull'],
            'aqua_total_reward_amount_equivalent': ['lt', 'gt'],
        }
