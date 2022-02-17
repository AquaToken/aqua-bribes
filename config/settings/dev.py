from config.settings.base import *  # noqa: F403

DEBUG = True
TEMPLATES[0]['OPTIONS']['debug'] = DEBUG

SECRET_KEY = env('SECRET_KEY', default='test_key')

ALLOWED_HOSTS = ['*']
INTERNAL_IPS = ['127.0.0.1']

ADMINS = (
    ('Dev Email', env('DEV_ADMIN_EMAIL', default='admin@localhost')),
)
MANAGERS = ADMINS


# Database
# https://docs.djangoproject.com/en/1.9/ref/settings/#databases
# --------------------------------------------------------------------------

DATABASES = {
    'default': env.db(default='postgres://localhost/aquarius_bribes'),
}


# Email settings
# --------------------------------------------------------------------------

DEFAULT_FROM_EMAIL = 'noreply@example.com'
SERVER_EMAIL = DEFAULT_FROM_EMAIL
EMAIL_BACKEND = 'django.core.mail.backends.console.EmailBackend'

if CELERY_ENABLED:
    MAILING_USE_CELERY = False


# Debug toolbar installation
# --------------------------------------------------------------------------

INSTALLED_APPS += (
    'debug_toolbar',
)

MIDDLEWARE += [
    'debug_toolbar.middleware.DebugToolbarMiddleware',
]
INTERNAL_IPS = ('127.0.0.1',)


if CELERY_ENABLED:
    # Celery configurations
    # http://docs.celeryproject.org/en/latest/configuration.html
    # --------------------------------------------------------------------------

    CELERY_BROKER_URL = env('CELERY_BROKER_URL', default='amqp://guest@localhost//')

    CELERY_TASK_ALWAYS_EAGER = True


# New Relic configurations
# --------------------------------------------------------------------------

NEWRELIC_DJANGO_ACTIVE = False

# Sentry config
# -------------

SENTRY_ENABLED = False


# Horizon configuration
# --------------------------------------------------------------------------

STELLAR_PASSPHRASE = 'Test SDF Network ; September 2015'
HORIZON_URL = 'https://horizon-testnet.stellar.org'


# Bribe configuration
# --------------------------------------------------------------------------

BRIBE_WALLET_ADDRESS = 'GBE2IGEEOPXHGJLGFL4U3SHMMKR5QXB5Q2YAV2MA36QGLY3GNXHSGPF3'
BRIBE_WALLET_SIGNER = 'SDNWIYEBRLSR2L65ADEH3AYPF45OMGNSXNOQ6KIKESR4VRKD4N5LAK7P'
