#!/usr/bin/env python3
from re import search
import time
import queue
import threading
import logging

from .class_util import Article, Search_page, Author
from .database_util import (insert_article_data, insert_search,
                            get_id_less_authors, get_search, update_author_scopus,
                            update_search_offset, connect_search_article)
from flask import Flask
from celery import Celery

logger = logging.getLogger('mainLogger')

do_bibtex = False

app = Flask(__name__)
app.config['SECRET_KEY'] = 'top top secret!'

app.config['CELERY_BROKER_URL'] = 'redis://0.0.0.0:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://0.0.0.0:6379/0'
app.config['CELERY_IGNORE_RESULT'] = False

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])

celery.conf.update(app.config)


@celery.task(bind=True, name='scopus_search')
def scopus_search(self):
    logger.debug('[get_sd_ou][scopus_search][IN] | ')
    logger.debug('[Scopus_search] started')
    names = get_id_less_authors()
    count = 0

    for name in names:
        last, first = name.split('|')
        name = {'last_name': last, 'first_name': first}
        logger.debug(f'getting scopus | name:{name}')
        author = Author(**name, do_scopus=True)
        update_author_scopus(name=author['name'], id=author['id'])
        count += 1

    logger.debug('[get_sd_ou][scopus_search][OUT] | authors_count : %s', count)


def get_next_page(queue_id='', **search_kwargs):
    logger.debug(
        '[get_sd_ou][get_next_page][IN] | search_kwargs : %s', search_kwargs)
    count = 0
    while True:
        search_obj = Search_page(**search_kwargs)
        while search_obj:
            res = {'search_page': search_obj, 'index_current_page': search_obj.curent_page_num,
                   'page_count': search_obj.pages_count}
            count += 1
            logger.debug('[get_sd_ou][get_next_page][RETURN] | res : %s', res)
            yield res
            search_obj = search_obj.next_page()
        if not search_kwargs.get('date'):
            break
        search_kwargs['date'] = str(int(search_kwargs.get('date')) - 1)
    logger.debug('[get_sd_ou][get_next_page][OUT] | count : %s', count)
    return {'result': 'done', 'index_current_page': search_obj.curent_page_num, 'page_count': search_obj.pages_count}


def get_next_article(search_page):
    articles = search_page.get_articles()
    for article_num, article in enumerate(articles):
        res = {'search_page': Article(article), 'index_current_article': article_num,
               'articles_count': len(articles)}
        yield res


def get_prev_serach_offset(**search_kwargs):
    logger.debug('[get_sd_ou][get_prev_serach_offset][IN] | search_kwargs : %s', search_kwargs)
    search_hash = Search_page(**search_kwargs).db_hash()
    search = get_search(search_hash)
    if not search:
        search_kwargs['offset'] = 0
        search_id = insert_search(search_hash=search_hash, **search_kwargs)
        search = [search_id, 0]
    logger.debug('[get_sd_ou][get_prev_serach_offset][OUT] continue saved search | search_id : %s, offset : %s', search[0], search[-1])
    return search[0] , search[-1]
def insert_random_search():
    import random
    aff = ['iran', 'china', 'US', 'UK', 'iraq']
    date = ['2020', '1999', '2002', '2010', '2019']
    term = ['nano', 'bio', 'data', 'game']
    page = ['10', '20', '33']
    auth = ['ali', 'erfan', 'bagher', 'sara']
    for i in range(20):
        a = random.choice(aff)
        b = random.choice(date)
        c = random.choice(term)
        d = random.choice(page)
        e  = random.choice(auth)

        get_prev_serach_offset(**{'affiliation':a, 'date':b, 'qs':c, 'page':d, 'authors':e})
@celery.task(bind=True, name='start_search')
def start_search(self, **search_kwargs):
    logger.debug('[get_sd_ou][start_search][IN] | search_kwargs : %s', search_kwargs)
    search_id, search_kwargs['offset'] = get_prev_serach_offset(**search_kwargs)
    first_page = True
    count = 0

    for page_res in get_next_page(**search_kwargs):
        page, index_current_page, pages_count = page_res.values()
        if not first_page:
            update_search_offset(hash=page.db_hash(), offset=page.offset)
        self.update_state(state='PROGRESS',
                          meta={'current': index_current_page, 'total': pages_count,
                                'status': f'Getting page articles\n{page.url}'})
        if page == 'done':
            return 'DONE'
        for article_res in get_next_article(page):
            article, index_current_article, articles_count = article_res.values()
            article_data = article.get_article_data()
            article_id = insert_article_data(**article_data)
            connect_search_article(search_id, article_id)
            self.update_state(state='PROGRESS',
                              meta={'current': index_current_page, 'total': pages_count,
                                    'status': f'{index_current_article}/{articles_count} \n {article.url}'})
            count += 1
            time.sleep(0.1)
        first_page = False
    logger.debug('[get_sd_ou][start_search][OUT] | count : %s', count)

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
