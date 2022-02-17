from django.conf import settings

from stellar_sdk import Server


def get_horizon():
    server = Server(settings.HORIZON_URL)
    return server
