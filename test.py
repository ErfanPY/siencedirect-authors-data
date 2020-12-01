import redis
from celery import Celery

from get_sd_ou.get_sd_ou import start_multi_search
from get_sd_ou.get_sd_ou import start_search, start_multi_search

import logging

logger = logging.getLogger('mainLogger')

class redis_test:
    def __init__(self):
        self.redisClient = redis.StrictRedis(host='localhost', port=6379, db=0)

        self.colorSet = "Colors"
    
    def add_test(self):
        self.redisClient.sadd(self.colorSet, "Red")
        self.redisClient.sadd(self.colorSet, "Orange")
        self.redisClient.sadd(self.colorSet, "Yellow")
        self.redisClient.sadd(self.colorSet, "Green")
        self.redisClient.sadd(self.colorSet, "Blue")
        self.redisClient.sadd(self.colorSet, "Indigo")
        self.redisClient.sadd(self.colorSet, "violet")

    def get_test(self):
        self.redisClient = redis.StrictRedis(host='localhost', port=6379, db=0)

        self.colorSet = "Colors"
        
        print("Cardinality of the Redis set:")
        print(self.redisClient.scard(self.colorSet))
        print("Contents of the Redis set:")

        for i in self.redisClient.smembers(self.colorSet):
            if bytes('Orange', encoding='UTF-8') == i:
                input(i)
            else:
                print('Not', i)

    def check_revoke(self):
        print(self.redisClient.smembers('celery_revoke'))

class celery_test:
    def __init__(self):
        self.config = {}
        self.config['SECRET_KEY'] = 'top top secret!'

        self.config['CELERY_BROKER_URL'] = 'redis://127.0.0.1:6379/0'
        self.config['CELERY_RESULT_BACKEND'] = 'redis://127.0.0.1:6379/0'
        self.config['CELERY_IGNORE_RESULT'] = False

        self.celery = Celery(__name__, broker=self.config['CELERY_BROKER_URL'])

        self.celery.conf.update(self.config)

    def multiple_search(self):
        search_kwargs = {'qs':'nano'}
        _ = start_multi_search.apply_async(kwargs={"worker_count":3, **search_kwargs},
                                    queue="main_search")

class class_util_test:
    def __init__ (self, article_key):
        self.article_dict = {
            'last_name':'https://www.sciencedirect.com/science/article/pii/S2211715620300242',
            '':'https://www.sciencedirect.com/science/article/abs/pii/S1540748920304715#!',
        }
    def article(self, article_key=None):
        from get_sd_ou.class_util import Article
        if article_key:
            article = Article(self.article_dict.get(article_key))
            print(article.authors)
        else:
            for article_url in self.article_dict.values():
                
redis_test().check_revoke()