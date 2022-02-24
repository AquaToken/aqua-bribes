from django.utils import timezone

from datetime import datetime

from rest_framework.generics import GenericAPIView
from rest_framework.mixins import ListModelMixin
from rest_framework.permissions import AllowAny

from aquarius_bribes.bribes.models import AggregatedByAssetBribe, Bribe
from aquarius_bribes.bribes.pagination import CustomPagination
from aquarius_bribes.bribes.serializers import AggregatedByAssetBribeSerializer
from aquarius_bribes.utils.filters import MultiGetFilterBackend


class AggregatedByAssetBribeListView(ListModelMixin, GenericAPIView):
    serializer_class = AggregatedByAssetBribeSerializer
    permission_classes = (AllowAny, )
    pagination_class = CustomPagination
    filter_backends = (MultiGetFilterBackend, )
    multiget_filter_fields = ('market_key', )
    timestamp_param = 'timestamp'

    def get_queryset(self):
        timestamp = self.request.query_params.get(self.timestamp_param)
        if timestamp:
            try:
                timestamp = datetime.utcfromtimestamp(int(timestamp)).replace(tzinfo=timezone.utc)
            except (ValueError, OverflowError):
                raise ParseError()
        else:
            timestamp = timezone.now()

        start_at = timestamp.replace(day=timestamp.day - timestamp.isoweekday() + 1)
        start_at = start_at.replace(hour=0, minute=0, second=0, microsecond=0)
        stop_at = start_at + Bribe.DEFAULT_DURATION
        return AggregatedByAssetBribe.objects.filter(start_at=start_at, stop_at=stop_at).order_by('-created_at')

    def get(self, request, *args, **kwargs):
        return self.list(request, *args, **kwargs)
