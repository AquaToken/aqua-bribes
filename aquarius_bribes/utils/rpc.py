from django.conf import settings

from stellar_sdk import SorobanServer


def get_rpc_server():
    server = SorobanServer(settings.SOROBAN_RPC_URL)
    server._client.request_timeout = settings.DEFAULT_GET_TIMEOUT
    server._client._session.headers.update({'User-Agent': settings.HORIZON_REQUEST_USER_AGENT})
    return server
