from django.urls import path

from aquarius_bribes.bribes.api import MarketKeyBribeListView, PendingBribeListView

urlpatterns = [
    path('bribes/', MarketKeyBribeListView.as_view()),
    path('pending-bribes/', PendingBribeListView.as_view()),
]
