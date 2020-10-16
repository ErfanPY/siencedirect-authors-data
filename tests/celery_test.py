# Test updating the task status from a function called in task
from celery import Celery
import time

config = {}

config['CELERY_BROKER_URL'] = 'redis://0.0.0.0:6379/0'
config['CELERY_RESULT_BACKEND'] = 'redis://0.0.0.0:6379/0'
config['CELERY_IGNORE_RESULT'] = False

celery = Celery(__name__, broker=config['CELERY_BROKER_URL'])

celery.conf.update(config)


def do_it(self):
    while True:
        time.sleep(1)
        print('Hello')
        self.update_state(state='PROGRESS',
                          meta={'status': 'Doing'})


@celery.task(bind=True, name='main')
def main(self):
    do_it(self)
