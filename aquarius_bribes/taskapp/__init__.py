import os

from django.conf import settings

from celery import Celery
from celery.schedules import crontab

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
        'aquarius_bribes.bribes.tasks.task_load_bribes': {
            'task': 'aquarius_bribes.bribes.tasks.task_load_bribes',
            'schedule': crontab(hour='*', minute='*/5'),
            'args': (),
        },
        'aquarius_bribes.bribes.tasks.task_update_pending_bribe_aqua_equivalent': {
            'task': 'aquarius_bribes.bribes.tasks.task_update_pending_bribe_aqua_equivalent',
            'schedule': crontab(minute='*/10'),
            'args': (),
        },
        'aquarius_bribes.bribes.tasks.task_update_pending_bribe_period': {
            'task': 'aquarius_bribes.bribes.tasks.task_update_pending_bribe_period',
            'schedule': crontab(hour='0', minute='0', day_of_week='monday'),
            'args': (),
        },
        'aquarius_bribes.bribes.tasks.task_stop_active_bribes': {
            'task': 'aquarius_bribes.bribes.tasks.task_stop_active_bribes',
            'schedule': crontab(hour='0', minute='0', day_of_week='monday'),
            'args': (),
        },
        'aquarius_bribes.bribes.tasks.task_return_bribes': {
            'task': 'aquarius_bribes.bribes.tasks.task_return_bribes',
            'schedule': crontab(hour='9', minute='0', day_of_week='sunday'),
            'args': (),
        },
        'aquarius_bribes.bribes.tasks.task_claim_bribes': {
            'task': 'aquarius_bribes.bribes.tasks.task_claim_bribes',
            'schedule': crontab(hour='19', minute='0', day_of_week='sunday'),
            'args': (),
        },
        'aquarius_bribes.bribes.tasks.task_aggregate_bribes': {
            'task': 'aquarius_bribes.bribes.tasks.task_aggregate_bribes',
            'schedule': crontab(hour='20', minute='0', day_of_week='sunday'),
            'args': (),
        },
        # 'aquarius_bribes.rewards.tasks.task_run_load_votes': {
        #     'task': 'aquarius_bribes.rewards.tasks.task_run_load_votes',
        #     'schedule': crontab(hour='0', minute='0'),
        #     'args': (),
        # },
        'aquarius_bribes.rewards.tasks.task_make_trustees_snapshot': {
            'task': 'aquarius_bribes.rewards.tasks.task_make_trustees_snapshot',
            'schedule': crontab(hour='0', minute='0'),
            'args': (),
        },
        'aquarius_bribes.bribes.tasks.task_update_bribe_aqua_equivalent': {
            'task': 'aquarius_bribes.bribes.tasks.task_update_bribe_aqua_equivalent',
            'schedule': crontab(hour='*', minute='0'),
            'args': (),
        },
        'aquarius_bribes.rewards.tasks.task_pay_rewards': {
            'task': 'aquarius_bribes.rewards.tasks.task_pay_rewards',
            'schedule': crontab(hour='*', minute='1'),
            'args': (),
        },
        'drf_secure_token.tasks.delete_old_tokens': DELETE_OLD_TOKENS,
    })
