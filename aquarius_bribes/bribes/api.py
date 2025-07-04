from datetime import datetime, timedelta

from django.db.models import Prefetch, Q, Sum
from django.utils import timezone

from rest_framework.exceptions import ParseError
from rest_framework.filters import OrderingFilter
from rest_framework.generics import GenericAPIView
from rest_framework.mixins import ListModelMixin
from rest_framework.permissions import AllowAny

from django_filters.rest_framework import DjangoFilterBackend

from aquarius_bribes.bribes.filters import BribeFilter
from aquarius_bribes.bribes.models import AggregatedByAssetBribe, Bribe, MarketKey
from aquarius_bribes.bribes.pagination import CustomPagination
from aquarius_bribes.bribes.serializers import BribeSerializer, MarketKeySerializer
from aquarius_bribes.utils.filters import MultiGetFilterBackend


class MarketKeyBribeListView(ListModelMixin, GenericAPIView):
    serializer_class = MarketKeySerializer
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

        start_at = timestamp - timedelta(days=timestamp.isoweekday() - 1)
        start_at = start_at.replace(hour=0, minute=0, second=0, microsecond=0)
        stop_at = start_at + Bribe.DEFAULT_DURATION

        return MarketKey.objects.annotate(
            aqua_sum=Sum(
                'aggregated_bribes__aqua_total_reward_amount_equivalent', filter=Q(
                    aggregated_bribes__start_at=start_at, aggregated_bribes__stop_at=stop_at
                )
            )
        ).filter(aqua_sum__gt=0).order_by('-aqua_sum').prefetch_related(
            Prefetch(
                'aggregated_bribes',
                queryset=AggregatedByAssetBribe.objects.filter(
                    start_at=start_at, stop_at=stop_at,
                ),
            ),
        )

    def get(self, request, *args, **kwargs):
        return self.list(request, *args, **kwargs)


class PendingBribeListView(ListModelMixin, GenericAPIView):
    serializer_class = BribeSerializer
    permission_classes = (AllowAny, )
    pagination_class = CustomPagination
    filter_backends = (DjangoFilterBackend, OrderingFilter,)
    filterset_class = BribeFilter
    ordering_fields = (
        'start_at', 'stop_at', 'unlock_time', 'market_key', 'amount', 'aqua_total_reward_amount_equivalent',
    )

    def get_queryset(self):
        return Bribe.objects.filter(
            Q(status=Bribe.STATUS_PENDING) | Q(status=Bribe.STATUS_ACTIVE, start_at__gt=timezone.now())
        )

    def get(self, request, *args, **kwargs):
        return self.list(request, *args, **kwargs)
