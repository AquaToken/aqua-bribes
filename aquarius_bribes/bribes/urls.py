from django.urls import path

from aquarius_bribes.bribes.api import AggregatedByAssetBribeListView


urlpatterns = [
    path('bribes/', AggregatedByAssetBribeListView.as_view()),
]
