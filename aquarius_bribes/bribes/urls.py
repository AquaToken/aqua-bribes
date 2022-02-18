from django.urls import path

from aquarius_bribes.bribes.api import BribeListView


urlpatterns = [
    path('bribes/', BribeListView.as_view()),
]
