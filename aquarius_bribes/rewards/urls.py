from django.urls import path

from aquarius_bribes.rewards.api import RunVotesSnapshotAPIView


urlpatterns = [
    path('take-votes-snapshot/', RunVotesSnapshotAPIView.as_view()),
]
