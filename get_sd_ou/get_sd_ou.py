#!/usr/bin/env python3
import time
import queue
import threading
import logging

from .class_util import Article, Search_page, Author
#from .database_util import insert_article_data, get_id_less_authors, update_author_scopus
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


@celery.task(bind=True, name='scopus_search')
def scopus_search(self):
    logger.debug('[Scopus_search] started')
    names = get_id_less_authors()

    for name in names:
        last, first = name.split('|')
        name = {'last_name': last, 'first_name': first}
        logger.debug(f'getting scopus | name:{name}')
        author = Author(**name, do_scopus=True)
        update_author_scopus(name=author['name'], id=author['id'])


def init_queue():
    global main_queue
    main_queue = queue.Queue()
    return main_queue


def get_from_queue():
    res = main_queue.get()
    while res:
        yield res
        res = main_queue.get()


def put_to_queue(task):
    main_queue.put(task)


def get_next_page(queue_id='', **search_kwargs):
    while True:
        search_obj = Search_page(**search_kwargs)
        while search_obj:
            res = {'search_page': search_obj, 'index_current_page': search_obj.curent_page_num,
                   'page_count': search_obj.pages_count}
            yield res
            search_obj = search_obj.next_page()
        if not search_kwargs.get('date'):
            break
        search_kwargs['date'] = str(int(search_kwargs.get('date')) - 1)

    return {'result': 'done', 'index_current_page': search_obj.curent_page_num, 'page_count': search_obj.pages_count}


def get_next_article(search_page):
    articles = search_page.get_articles()
    for article_num, article in enumerate(articles):
        res = {'search_page': Article(article), 'index_current_article': article_num,
               'articles_count': len(articles)}
        yield res


@celery.task(bind=True, name='start_search')
def start_search(self, **search_kwargs):
    for page_res in get_next_page(**search_kwargs):
        print('########', page_res, '\n\n\n')
        page, index_current_page, pages_count = page_res.values()
        self.update_state(state='PROGRESS',
                          meta={'current': index_current_page, 'total': pages_count,
                                'status': f'Getting page articles\n{page.url}'})
        if page == 'done':
            return 'DONE'
        for article_res in get_next_article(page):
            article, index_current_article, articles_count = article_res.values()
            article_data = article.get_article_data()
            # insert_article_data(**article_data)
            self.update_state(state='PROGRESS',
                              meta={'current': index_current_page, 'total': pages_count,
                                    'status': f'{index_current_article}/{articles_count} \n {article.url}'})


if __name__ == "__main__":
    logger.debug(
        '___________________________________________[ Search Start ]________________________________________')
    start_search(2020)
