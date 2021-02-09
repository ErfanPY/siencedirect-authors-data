#!/usr/bin/env python3
import time
import logging

from get_sd_ou.classUtil import Article, SearchPage, Author
from get_sd_ou.databaseUtil import (init_db, insert_article_data, insert_search,
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
    db_connection = init_db()
    names = get_id_less_authors(cnx=db_connection)
    count = 0

    for name in names:
        last, first = name.split('|')
        name = {'last_name': last, 'first_name': first}
        logger.debug(f'getting scopus | name:{name}')
        author = Author(**name, do_scopus=True)
        update_author_scopus(
            name=author['name'], scopus_id=author['id'], cnx=db_connection)
        count += 1

    logger.debug('[get_sd_ou][scopus_search][OUT] | authors_count : %s', count)


def get_next_page(start_offset=0, **search_kwargs):
    count = 0
    while True:
        search_obj = SearchPage(start_offset=start_offset, **search_kwargs)
        while search_obj:
            res = {'SearchPage': search_obj, 'index_current_page': search_obj.curent_page_num,
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


def get_next_article(SearchPage):
    articles = SearchPage.get_articles()
    for article_num, article in enumerate(articles):
        res = {'SearchPage': Article(article), 'index_current_article': article_num,
               'articles_count': len(articles)}
        yield res


def get_prev_serach_offset(db_connection, **search_kwargs):
    search_hash = SearchPage(**search_kwargs).db_hash()
    search = get_search(search_hash, cnx=db_connection)
    if not search:
        search_kwargs['offset'] = 0
        search_id = insert_search(
            search_hash=search_hash, **search_kwargs, cnx=db_connection)
        search = {'search_id': search_id, 'offset': 0}
    logger.debug(
        '[get_sd_ou][get_prev_serach_offset][OUT] continue saved search | search_id : %s, offset : %s', search['search_id'], search['offset'])
    return search['search_id'], search['offset']


@celery.task(bind=True, name='start_multi_search')
def start_multi_search(self, worker_count=1, worker_offset_count=100, **search_kwargs):
    db_connection = init_db()

    search_id, offset = get_prev_serach_offset(
        **search_kwargs, db_connection=db_connection)

    db_connection.close()

    task_id_list = []

    logger.debug(
        '[get_sd_ou][start_multi_search][MIDDLE] | Prev search got | search_id : %s, offset : %s', search_id, offset)
    task = start_search.apply_async(kwargs={"write_offset": False, 'search_id': search_id,
                                            'start_offset': offset,
                                            **search_kwargs},
                                    queue="main_search")
    task_id_list.append(task.id)
    logger.debug(
        '[get_sd_ou][start_multi_search][MIDDLE] | First task started | search_kwargs : %s', search_kwargs)

    for i in range(worker_count-1):
        task = start_search.apply_async(kwargs={"write_offset": False, 'search_id': search_id,
                                                'start_offset': offset+(worker_offset_count*(i+1)),
                                                **search_kwargs},
                                        queue="main_search")
        task_id_list.append(task.id)
        logger.debug(
            '[get_sd_ou][start_multi_search][MIDDLE] | Another task started | search_kwargs : %s, i : %s', search_kwargs, i)
    return task_id_list


@celery.task(bind=True, name='start_search')
def start_search(self, write_offset=True, search_id=0, start_offset=0, end_offset=-1, db_connection=None, **search_kwargs):
    db_connection = db_connection if db_connection else init_db()

    task_id = self.request.id.__str__()

    if not start_offset:
        search_id, search_kwargs['offset'] = get_prev_serach_offset(
            **search_kwargs, db_connection=db_connection)
    else:
        search_id = search_id
        search_kwargs['offset'] = start_offset

    count = 0
    cleaned_search_kwargs = {k: v for k, v in search_kwargs.items() if v not in [
        '', ' ', [], None]}
    cleaned_search_kwargs_reper = " | ".join(
        [': '.join([k, str(v)]) for k, v in cleaned_search_kwargs.items()])
    for page_res in get_next_page(start_offset=start_offset, **search_kwargs):
        page, index_current_page, pages_count = page_res.values()
        page_hash = page.db_hash()

        self.update_state(state='PROGRESS',
                          meta={'current': index_current_page, 'total': pages_count,
                                'status': f'Getting page articles\n{page.url}', 'form': 'form'})

        if page == 'done':
            return 'DONE'
        if end_offset != -1 and int(page.offset) > end_offset:
            logger.debug('[get_sd_ou][start_search][RETURN] Search reached end_offset | offset : %s, task_id : %s, search_kwargs : %s',
                         page.offset, task_id, search_kwargs)
            return 'Finished'

        for article_res in get_next_article(page):

            if bytes(task_id, encoding='UTF-8') in redisClient.smembers('celery_revoke'):
                logger.debug(
                    '[get_sd_ou][start_search][RETURN] Task removed | task_id : %s, search_kwargs : %s', task_id, search_kwargs)
                return 'Removed'

            article, index_current_article, articles_count = article_res.values()

            if is_article_exist(article.pii, cnx=db_connection):
                logger.debug(
                    '[get_sd_ou][start_search][MIDDLE] Article exist | pii : %s', article.pii)
                continue

            article_data = article.get_article_data()
            article_id = insert_article_data(**article_data, cnx=db_connection)

            connect_search_article(search_id, article_id, cnx=db_connection)

            page.offset = str(int(page.offset)+1)

            if write_offset:
                update_search_offset(
                    search_hash=page_hash, offset=page.offset, cnx=db_connection)

            self.update_state(state='PROGRESS',
                              meta={'current': index_current_page, 'total': pages_count,
                                    'status': f'Searching with this Fields: {cleaned_search_kwargs_reper}<br />{index_current_article}/{articles_count} Article<br /> {article.url}'})
            count += 1
            time.sleep(1)
        _first_page = False
    logger.debug('[get_sd_ou][start_search][OUT] | count : %s', count)
