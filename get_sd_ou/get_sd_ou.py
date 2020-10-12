import queue
import threading
import logging

from .class_util import Article, Search_page
from .database_util import insert_article_data
from flask import Flask
from celery import Celery

logger = logging.getLogger('mainLogger')

app = Flask(__name__)
app.config['SECRET_KEY'] = 'top top secret!'

app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'
app.config['CELERY_IGNORE_RESULT'] = False

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])

celery.conf.update(app.config)

def printer(a):
    print(a)

def next_year_gen(start_year=2020, year_step=-1, **search_kwargs):
    """ iterate through all year fron start_year """
    current_search_year = start_year
    while(True):
        yield {'year':current_search_year, 'search_kwrgs':search_kwargs}
        current_search_year += year_step

def next_page_gen(year, **kwargs):
    """ iterate through page of year """
    page = 1
    logger.debug('[ main ] [ next_page_gen ] __init__ | year: %s, page=%s', year, page)
    search_obj = Search_page(year, **kwargs)
    while True :
        logger.debug('[ main ] [ next_page_gen ] next page | year: %s, page=%s, url: %s', year, page, search_obj.url)
        res = {'search_page':search_obj, 'page_number':page, 'year':year}
        yield res
        page += 1
        search_obj = search_obj.next_page()

def worker(**search_kwargs):
    global continue_search
    global next_page_gen_obj
    year = next(next_year_gen_obj)['year']
    next_page_gen_obj = next_page_gen(year , **search_kwargs)
    continue_search = True

    while continue_search:
        if main_queue.empty():
            search_page, page_number, year = next(next_page_gen_obj).values()
            if search_page:
                logger.debug('[ worker ] get artciles from year: %s , page: %s', year, page_number)
                
                articles = search_page.get_articles()
                [main_queue.put(article) for article in articles]

                logger.debug('[ worker ] page artciles got | year: %s , page: %s', year, page_number)
            else:
                
                next_year, search_kwargs = next(next_year_gen_obj)
                logger.debug('[ worker ] go to next year search: %s', next_year)
                next_page_gen_obj = next_page_gen(next_year, **search_kwargs)
                continue
            
        article_url = main_queue.get()
        article = Article(article_url)
        #logger.debug('[ worker ] get data of article | pii : %s', article.pii)
        article_data = article.get_article_data()
        logger.info('Article authors got , %s', article_data)
        insert_article_data(**article_data)
        
        main_queue.task_done()
@celery.task(bind=True)
def start_search(self, **search_kwargs):
    #global threads
    global main_queue
    global next_year_gen_obj
    """ 
    1) Initiate the threads
    2) Initiate the database connection
    3) Call the worker function of each thread
    """
    start_year = search_kwargs['start_year']
    self.update_state(state='PROGRESS',
                          meta={'current': 1, 'total': 100,
                                'status': 'in get sd ou'})
    next_year_gen_obj = next_year_gen(start_year=start_year)
    main_queue = queue.Queue()
    #threads = [threading.Thread(target=worker, kwargs=search_kwargs) for _ in range(2)]
    #[thread.start() for thread in threads]
    worker(**search_kwargs)
    main_queue.join()

def pause_search():
    logger.info('[ main ] search paused')
    pass

def stop_search():
    """ 
    1) Commit current state to database
    2) kill threads
    """
    logger.info('[ main ] search stoped')
    pass

if __name__ == "__main__":
    logger.debug('___________________________________________[ Search Start ]________________________________________')
    start_search(2020)
