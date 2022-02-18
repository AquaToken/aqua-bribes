from rest_framework.generics import GenericAPIView
from rest_framework.mixins import ListModelMixin
from rest_framework.permissions import AllowAny

from aquarius_bribes.bribes.models import Bribe
from aquarius_bribes.bribes.pagination import CustomPagination
from aquarius_bribes.bribes.serializers import BribeSerializer


class BribeListView(ListModelMixin, GenericAPIView):
    serializer_class = BribeSerializer
    permission_classes = (AllowAny, )
    pagination_class = CustomPagination

    def get_queryset(self):
        return Bribe.objects.filter(status__in=(Bribe.STATUS_PENDING, Bribe.STATUS_ACTIVE))

    def get(self, request, *args, **kwargs):
        return self.list(request, *args, **kwargs)
