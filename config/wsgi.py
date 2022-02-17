"""
WSGI config for test_project project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/2.2/howto/deployment/wsgi/
"""

import os

import newrelic.agent
from newrelic.api.exceptions import ConfigurationError

try:
    newrelic.agent.initialize(os.path.join(os.path.dirname(__file__), '..', 'conf', 'newrelic.ini'))
except ConfigurationError:
    newrelic_initialized = False
else:
    newrelic_initialized = True


from django.core.wsgi import get_wsgi_application  # noqa I001

application = get_wsgi_application()

# don't move it before newrelic agent initialization because modules loaded before initialize will be uninstrumented
from django.conf import settings  # noqa E402, I001

if settings.NEWRELIC_DJANGO_ACTIVE:
    if not newrelic_initialized:
        raise ConfigurationError()

    application = newrelic.agent.WSGIApplicationWrapper(application)
