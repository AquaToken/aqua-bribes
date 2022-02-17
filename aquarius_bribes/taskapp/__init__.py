import os

from django.conf import settings

from celery import Celery
from datetime import timedelta

if not settings.configured:
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings.dev')

app = Celery('aquarius_bribes')

app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)
app.conf.timezone = 'UTC'


@app.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):
    from drf_secure_token.tasks import DELETE_OLD_TOKENS

    app.conf.beat_schedule.update({
        'aquarius_bribes.testapp.tasks.test_task': {
            'task': 'aquarius_bribes.testapp.tasks.test_task',
            'run_every': timedelta(seconds=10),
            'args': (),
        },
        'drf_secure_token.tasks.delete_old_tokens': DELETE_OLD_TOKENS,
    })
