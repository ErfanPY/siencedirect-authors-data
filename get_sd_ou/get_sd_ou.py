#!/usr/bin/env python3
import queue
from re import search
import threading
import logging

from .class_util import Article, Search_page, Author
from .database_util import insert_article_data, get_id_less_authors, update_author_scopus
from flask import Flask
from celery import Celery

logger = logging.getLogger('mainLogger')

app = Flask(__name__)
app.config['SECRET_KEY'] = 'top top secret!'

app.config['CELERY_BROKER_URL'] = 'redis://0.0.0.0:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://0.0.0.0:6379/0'
app.config['CELERY_IGNORE_RESULT'] = False

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])

celery.conf.update(app.config)


def next_year_gen(start_year, year_step=-1, **search_kwargs):
    logger.debug('next_year_gen')
    """ iterate through all year fron start_year """
    current_search_year = start_year
    while(True):
        yield {'year': current_search_year, 'search_kwrgs': search_kwargs}
        current_search_year += year_step


def next_page_gen(**search_kwargs):
    logger.debug('next_page_gen')
    """ iterate through page of year """
    page = 1
    logger.debug('[ main ] [ next_page_gen ] __init__ | page=%s', page)
    search_obj = Search_page(**search_kwargs)
    while True:
        logger.debug(
            '[ main ] [ next_page_gen ] next page | page=%s, url: %s', page, search_obj.url)
        res = {'search_page': search_obj, 'page_number': page}
        yield res
        page += 1
        search_obj = search_obj.next_page()


def worker(**search_kwargs):
    logger.debug('worker')
    global continue_search
    global next_page_gen_obj
    year = next(next_year_gen_obj)['year']
    search_kwargs['date'] = year
    next_page_gen_obj = next_page_gen(**search_kwargs)
    continue_search = True

    while continue_search:
        if main_queue.empty():
            search_page, page_number = next(next_page_gen_obj).values()
            if search_page:
                logger.debug(
                    '[ worker ] get artciles from year: %s , page: %s', year, page_number)

                articles = search_page.get_articles()
                [main_queue.put(article) for article in articles]

                logger.debug(
                    '[ worker ] page artciles got | year: %s , page: %s', year, page_number)
            else:

                next_year, search_kwargs = next(next_year_gen_obj)
                logger.debug(
                    '[ worker ] go to next year search: %s', next_year)
                search_kwargs['date'] = next_year
                next_page_gen_obj = next_page_gen(**search_kwargs)
                continue

        article_url = main_queue.get()
        article = Article(article_url)
        #logger.debug('[ worker ] get data of article | pii : %s', article.pii)
        article_data = article.get_article_data()
        logger.info('Article authors got , %s', article_data)
        insert_article_data(**article_data)

        main_queue.task_done()


def pages_worker(**search_kwargs):
    logger.debug('page worker')
    page_gen = next_page_gen(**search_kwargs)
    for page in page_gen:
        articles = page['search_page'].get_articles()
        for article_url in articles:
            data = Article(article_url).get_article_data()
            insert_article_data(**data)


@celery.task(bind=True, name='scopus_search')
def scopus_search(self):
    names = get_id_less_authors()
    for name in names:
        author = Author(**name, do_scopus=True)
        update_author_scopus(name=author['name'], id=author['id'])

def init_queue():
    global main_queue   
    main_queue = queue.Queue()
    return main_queue

def get_from_queue():
    while True:
        yield main_queue.get()

def put_to_queue(task):
    main_queue.put(task)

def done_to_queue():
    main_queue.task_done()

@celery.task(bind=True, name='start_search')
def start_search(self, **search_kwargs):
    global next_year_gen_obj
    init_queue()
    start_year = search_kwargs.get('date', '')
    self.update_state(state='PROGRESS',
                      meta={'current': 1, 'total': 100,
                            'status': 'Starting'})
    while True:
        for page_url in page_gen(**search_kwargs):
            for article_url in Search_page(page_url).articles:
                article = Article(article_url)
                article_data = article.get_article_data()
                insert_article_data(**article_data)

            
    if start_year == '':
        logger.debug('[main] [start_search] no year')
        search_kwargs['date'] = ''
        next_page_gen_obj = next_page_gen(**search_kwargs)
        continue_search = True
        logger.debug('[main] [start_search] no year')
        while continue_search:
            if main_queue.empty():
                search_page, page_number = next(next_page_gen_obj).values()
                if search_page:
                    logger.debug(
                        '[ worker ] get artciles from page: %s', page_number)

                    articles = search_page.get_articles()
                    [main_queue.put(article) for article in articles]

                    logger.debug(
                        '[ worker ] page artciles got | page: %s', page_number)
                else:
                    logger.debug('else')
                    self.update_state(state='PROGRESS',
                                      meta={'current': 100, 'total': 100,
                                            'status': 'All page got'})
                    return 0

            article_url = main_queue.get()
            article = Article(article_url)
            logger.debug(
                '[ worker ] get data of article | pii : %s', article.pii)
            article_data = article.get_article_data()
            logger.info('Article authors got , %s', article_data)
            status_text = '{} author got'.format(article.url)
            self.update_state(state='PROGRESS',
                              meta={'current': 2, 'total': 100,
                                    'status': status_text})

            insert_article_data(**article_data)

            main_queue.task_done()

    next_year_gen_obj = next_year_gen(start_year=start_year)
    year = next(next_year_gen_obj)['year']
    search_kwargs['date'] = year
    next_page_gen_obj = next_page_gen(**search_kwargs)
    continue_search = True
    main_queue = queue.Queue()
    while continue_search:
        if main_queue.empty():
            search_page, page_number = next(next_page_gen_obj).values()
            if search_page:
                logger.debug(
                    '[ worker ] get artciles from year: %s , page: %s', year, page_number)

                articles = search_page.get_articles()
                [main_queue.put(article) for article in articles]

                logger.debug(
                    '[ worker ] page artciles got | year: %s , page: %s', year, page_number)
            else:
                logger.debug('else')
                next_year, search_kwargs = next(next_year_gen_obj)
                logger.debug(
                    '[ worker ] go to next year search: %s', next_year)
                search_kwargs['date'] = next_year
                next_page_gen_obj = next_page_gen(**search_kwargs)
                continue
        article_url = main_queue.get()
        article = Article(article_url)
        logger.debug('[ worker ] get data of article | pii : %s', article.pii)
        article_data = article.get_article_data()
        logger.info('Article authors got , %s', article_data)
        status_text = '{} author got'.format(article.url)
        logger.debug('\n\n#######\nI am here\n##############\n\n')
        self.update_state(state='PROGRESS',
                          meta={'current': 2, 'total': 100,
                                'status': status_text})

        insert_article_data(**article_data)

        main_queue.task_done()

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
    logger.debug(
        '___________________________________________[ Search Start ]________________________________________')
    start_search(2020)
