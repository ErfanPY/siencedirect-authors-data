#!/usr/bin/env python3
from re import search
import re
import time
import queue
import threading
import logging

from get_sd_ou.class_util import Article, Search_page, Author
from get_sd_ou.database_util import (init_db, insert_article_data, insert_search,
                            get_id_less_authors, get_search, update_author_scopus,
                            update_search_offset, connect_search_article, is_article_exist)
import redis
from flask import Flask
from celery import Celery

logger = logging.getLogger('mainLogger')

do_bibtex = False

app = Flask(__name__)
app.config['SECRET_KEY'] = 'top top secret!'

app.config['CELERY_BROKER_URL'] = 'redis://127.0.0.1:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://127.0.0.1:6379/0'
app.config['CELERY_IGNORE_RESULT'] = False

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])

celery.conf.update(app.config)

redisClient = redis.StrictRedis(host='localhost', port=6379, db=0)


@celery.task(bind=True, name='scopus_search')
def scopus_search(self):
    logger.debug('[get_sd_ou][scopus_search][IN] | ')
    db_connection = init_db()
    names = get_id_less_authors(cnx=db_connection)
    count = 0

    for name in names:
        last, first = name.split('|')
        name = {'last_name': last, 'first_name': first}
        logger.debug(f'getting scopus | name:{name}')
        author = Author(**name, do_scopus=True)
        update_author_scopus(name=author['name'], id=author['id'], cnx=db_connection)
        count += 1

    logger.debug('[get_sd_ou][scopus_search][OUT] | authors_count : %s', count)


def get_next_page(queue_id='', start_offset=0, **search_kwargs):
    logger.debug(
        '[get_sd_ou][get_next_page][IN] | search_kwargs : %s', search_kwargs)
    count = 0
    while True:
        search_obj = Search_page(start_offset=start_offset, **search_kwargs)
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


def get_prev_serach_offset(db_connection, **search_kwargs):
    logger.debug(
        '[get_sd_ou][get_prev_serach_offset][IN] | search_kwargs : %s', search_kwargs)
    search_hash = Search_page(**search_kwargs).db_hash()
    search = get_search(search_hash, cnx=db_connection)
    if not search:
        search_kwargs['offset'] = 0
        search_id = insert_search(search_hash=search_hash, **search_kwargs, cnx=db_connection)
        search = {'search_id':search_id, 'offset':0}
    logger.debug(
        '[get_sd_ou][get_prev_serach_offset][OUT] continue saved search | search_id : %s, offset : %s', search['search_id'], search['offset'])
    return search['search_id'], search['offset']


def insert_random_search():
    import random
    aff = ['iran', 'china', 'US', 'UK', 'iraq', 'brazil',
           'arabestan', 'turkiey', 'tajikestan', 'holan', 'netherland']
    term = ['nano', 'bio', 'data', 'game', 'tech', 'computer', 'micro', 'AI']
    auth = ['ali', 'erfan', 'bagher', 'sara',
            'babak', 'mohamad', 'jack', 'jef']
    for _search in range(20):
        a = random.choice(aff)
        b = random.randrange(1990, 2020)
        c = random.choice(term)
        d = random.randrange(3, 900)
        e = random.choice(auth)
        search_id, _ = get_prev_serach_offset(
            **{'affiliation': a, 'date': b, 'qs': c, 'page': d, 'authors': e})
        for _ in range(random.randint(3, 6)):
            authors = [{'first_name': name, 'last_name': 'GH', 'email': name+'@gmail.com'} for name in auth[:random.randint(0, 8)]]
            article_id = insert_article_data(pii=''.join(random.sample(
                'ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890', 10)), authors=authors)
            connect_search_article(search_id, article_id)

@celery.task(bind=True, name='start_multi_search')
def start_multi_search(self, worker_count=1, worker_offset_count=100, **search_kwargs):
    logger.debug('[get_sd_ou][start_multi_search][IN] | search_kwargs : %s', search_kwargs)
    db_connection = init_db()
    
    search_id, offset = get_prev_serach_offset(
        **search_kwargs, db_connection=db_connection)
    
    db_connection.close()

    task_id_list = []

    logger.debug('[get_sd_ou][start_multi_search][MIDDLE] | Prev search got | search_id : %s, offset : %s', search_id, offset)
    task = start_search.apply_async(kwargs={"write_offset":False, 'search_id':search_id,
                                    'start_offset':offset,
                                    **search_kwargs},
                            queue="main_search")
    task_id_list.append(task.id)
    logger.debug('[get_sd_ou][start_multi_search][MIDDLE] | First task started | search_kwargs : %s', search_kwargs)

    for i in range(worker_count-1):
        task = start_search.apply_async(kwargs={"write_offset":False, 'search_id':search_id,
                                    'start_offset':offset+(worker_offset_count*(i+1)),
                                    **search_kwargs},
                            queue="main_search")
        task_id_list.append(task.id)
        logger.debug('[get_sd_ou][start_multi_search][MIDDLE] | Another task started | search_kwargs : %s, i : %s', search_kwargs, i)
    return task_id_list

"""
وقتی یه ورکر جدید که کلید واژه مشابهی رو سرچ می کنه اضافه میشه یه تابع تصمیم گیری بهش 
میگه که از چه افستی تا چه افستی رو باید سرچ کنه

"""

@celery.task(bind=True, name='start_search')
def start_search(self, write_offset=True, search_id=0, start_offset=0, end_offset= -1, db_connection=None, **search_kwargs):
    logger.debug(
        '[get_sd_ou][start_search][IN] | search_kwargs : %s', search_kwargs)
    db_connection = db_connection if db_connection else init_db()

    task_id = self.request.id.__str__()

    if not start_offset:
        search_id, search_kwargs['offset'] = get_prev_serach_offset(
            **search_kwargs, db_connection=db_connection)
    else :
        search_id = search_id
        search_kwargs['offset'] = start_offset
    
    #TODO: Don't write offset to db 

    _first_page = True
    count = 0
    cleaned_search_kwargs = {k:v for k, v in search_kwargs.items() if v not in ['', ' ', [], None]}
    cleaned_search_kwargs_reper  = " | ".join([': '.join([k, str(v)]) for k, v in cleaned_search_kwargs.items()])
    for page_res in get_next_page(start_offset=start_offset, **search_kwargs):
        page, index_current_page, pages_count = page_res.values()
        page_offset = page.offset
        page_hash = page.db_hash()

        self.update_state(state='PROGRESS',
                          meta={'current': index_current_page, 'total': pages_count,
                                'status': f'Getting page articles\n{page.url}', 'form':'form'})
        
        if page == 'done':
            return 'DONE'
        if end_offset != -1 and int(page_offset) > end_offset:
            logger.debug( '[get_sd_ou][start_search][RETURN] Search reached end_offset | offset : %s, task_id : %s, search_kwargs : %s', page_offset, task_id, search_kwargs)
            return 'Finished'

        for article_res in get_next_article(page):       
              
            if bytes(task_id, encoding='UTF-8') in redisClient.smembers('celery_revoke'):
                logger.debug( '[get_sd_ou][start_search][RETURN] Task removed | task_id : %s, search_kwargs : %s', task_id, search_kwargs)
                return 'Removed'

            article, index_current_article, articles_count = article_res.values()
            
            if is_article_exist(article.pii, cnx=db_connection):
                logger.debug( '[get_sd_ou][start_search][MIDDLE] Article exist | pii : %s', article.pii)
                continue

            article_data = article.get_article_data()
            article_id = insert_article_data(**article_data, cnx=db_connection)
        
            connect_search_article(search_id, article_id, cnx=db_connection)
        
            page_offset = str(int(page_offset)+1)
        
            if write_offset:
                update_search_offset(hash=page_hash, offset=page_offset, cnx=db_connection)
        
            self.update_state(state='PROGRESS',
                              meta={'current': index_current_page, 'total': pages_count,
                                  'status': f'Searching with this Fields: {cleaned_search_kwargs_reper}<br />{index_current_article}/{articles_count} Article<br /> {article.url}'})
            count += 1
            time.sleep(1)
        _first_page = False
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
