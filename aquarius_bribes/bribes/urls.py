from django.urls import path

from aquarius_bribes.bribes.api import MarketKeyBribeListView


urlpatterns = [
    path('bribes/', MarketKeyBribeListView.as_view()),
]
