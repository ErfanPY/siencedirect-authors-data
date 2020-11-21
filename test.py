import logging

logger = logging.getLogger('mainLogger')

class redis_test:
    def init(self):
        import redis
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
        import redis

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

class celery_test:
    def __init__(self):
        from get_sd_ou.get_sd_ou import start_search, start_multi_search

        from celery import Celery

        self.config = {}
        self.config['SECRET_KEY'] = 'top top secret!'

        self.config['CELERY_BROKER_URL'] = 'redis://127.0.0.1:6379/0'
        self.config['CELERY_RESULT_BACKEND'] = 'redis://127.0.0.1:6379/0'
        self.config['CELERY_IGNORE_RESULT'] = False

        self.celery = Celery(__name__, broker=self.config['CELERY_BROKER_URL'])

        self.celery.conf.update(self.config)

    def multiple_search(self):
        search_kwargs = {'qs':'nano'}
        a = start_multi_search.apply_async(kwargs={"worker_count":3, **search_kwargs},
                                    queue="main_search")

class class_util_test:
    def article(self):
        from get_sd_ou.class_util import Article
        article = Article('https://www.sciencedirect.com/science/article/abs/pii/S1540748920304715#!')
        print(article.authors)

class_util_test().article()