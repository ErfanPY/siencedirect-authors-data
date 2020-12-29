import asyncio
import json
import logging
import os
from urllib.parse import parse_qsl, urlparse

import aiohttp
# from aiohttp.helpers import current_task
from bs4 import BeautifulSoup as bs

from get_sd_ou.class_util import Article, Search_page
from get_sd_ou.database_util import (connect_search_article, get_article, get_search, init_db,
                                     insert_article_data, insert_search, update_article)


async def get_soup(session, url):
    async with session.get(url) as response:
        try:
            return await response.read()
        except Exception as e:
            return e


async def parse_search(session, search_url, search_name):
    search_content = await get_soup(session, search_url)
    if not search_content:
        return search_url, []
    search_soup = bs(search_content, 'html.parser')
    search_page = Search_page(url=search_url, soup_data=search_soup)

    articles = search_page.get_articles()
    logger.log(10001, search_url + ' || ' + str(len(articles)))
    ext_path = os.path.join('./extracted_articles', search_name+".json")
    with open(ext_path, 'r+') as file:
        try:
            search_dict = json.load(file)
        except json.decoder.JSONDecodeError:
            search_dict = {}

        search_dict[search_url] = list(
            set(search_dict.get(search_url, []) + articles))
        file.seek(0)
        json.dump(search_dict, file)
        file.truncate()
    return search_name, search_url, articles


def filter_search(search_url, free_or_limited_search):
    offset = int(dict(parse_qsl(urlparse(search_url).query)).get('offset', 0))
    if free_or_limited_search == 'f':
        return offset < 1000
    elif free_or_limited_search == 'l':
        return offset >= 1000
    else:
        return 1


async def start_searchs_parse(search_items):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for search_name, search_urls in search_items:
            for search_url in search_urls:
                task = asyncio.ensure_future(
                    parse_search(session, search_url, search_name))
                tasks.append(task)
            search_result = await asyncio.gather(*tasks, return_exceptions=True)
            # search_groups = [list(item[1]) for item in itertools.groupby(sorted(search_result), key=lambda x: x[0])]
            # for search_group in search_groups:
            #     search_name = search_group[0][0]
            #     with open(os.path.join('./extracted_articles', search_name+".json"), 'w+') as file:
            #         try :
            #             search_dict = json.load(file)
            #         except json.decoder.JSONDecodeError:
            #             search_dict = {}
            #         for _, search_url, articles in search_group:

            #             search_dict[search_url] = list(set(search_dict.get(search_url, []) + articles))
            #             json.dump(search_dict, file)
            #             logger.log(10001, f'{"END": >5} || {search_name} : {len(articles)} || {search_url}')


def is_in_extracted(search_name, url):
    with open(os.path.join('./extracted_articles', search_name+".json"), 'a+') as file:
        try:
            ext_search_dict = json.load(file)
        except json.decoder.JSONDecodeError:
            ext_search_dict = {}
        return len(ext_search_dict.get(url, [])) >= 10


def get_search_from_dir(dir, free_or_limited_search):

    search_dict = {}
    for file_name in os.listdir(dir):
        search_name = file_name.split('.')[0]
        file_path = os.path.join(dir, file_name)

        with open(file_path) as file:
            search_lines = [i.strip() for i in file.readlines() if i.strip()]
            search_urls = list(filter(lambda url: filter_search(url, free_or_limited_search) and
                                      not is_in_extracted(search_name, url),
                                      search_lines))
            search_dict[search_name] = search_urls
    return list(search_dict.items())

def log_formatter(msg, data):
    ret = ''
    ret += f'[{msg}]'
    for key, value in data.items():
        ret += f' {key}: {value} |'
    return ret[:-2]

async def parse_article(article_url, search_url, search_id, db_cnx, skip_article=True):

    return_data = {"search_url": search_url, "article_url": article_url, "search_id": search_id}
    logger.debug(log_formatter('parse_article|START', return_data))

    article_page = Article(url=article_url)
    article_data = get_article(article_page.pii, cnx=db_cnx)
    
    async with aiohttp.ClientSession() as session:
        try:
            article_content = await get_soup(session, article_url)
        except Exception as e:
            return e
        article_soup = bs(article_content, 'html.parser')
        article_page._soup = article_soup    

    if not article_data:
        article_id = insert_article_data(
            **article_page.get_article_data(), cnx=db_cnx)
    else:
        article_id = article_data.get('article_id')
        return_data["article_id"] = article_id
        
        if skip_article:
            return return_data

        update_article(
            **article_page.get_article_data(), cnx=db_cnx)

    connect_search_article(search_id=search_id,
                           article_id=article_id, cnx=db_cnx)
    logger.debug(log_formatter('parse_article|END', return_data))
    
    return return_data


async def start_articles_parse(searchs_dict):
    for search_name, search_article_dict in searchs_dict.items():
        tasks = []
        for search_url, articles in search_article_dict.items():
            with init_db() as db_cnx:
                search_page = Search_page(url=search_url)
                search_data = get_search(search_page.db_hash(), cnx=db_cnx)
            
                if not search_data:
                    search_id = insert_search(
                        search_hash=search_page.db_hash(), **search_page.search_kwargs, cnx=db_cnx)
                else:
                    search_id = search_data.get('search_id')

                logger.debug('[parse_article|START] search_name: %s, search_url: %s  | articles count: %s',
                            search_name, search_url, len(articles))

                
                for article in articles:
                    task = asyncio.ensure_future(parse_article(
                        article_url=article, search_url=search_url, search_id=search_id, db_cnx=db_cnx))
                    tasks.append(task)

                result = await asyncio.gather(*tasks, return_exceptions=True)

            logger.debug('[start_parse_article|END] search_name: %s, search_url: %s  | articles count: %s',
                         search_name, search_url, len(articles))
            logger.error('%s', result)


def get_articles_from_dir(dir_path):
    file_names = os.listdir(dir_path)

    searchs_dict = {}
    for file_name in file_names:
        file_path = os.path.join(dir_path, file_name)

        file_name = file_name.split('.')[0]
        with open(file_path) as file:
            try:
                searchs_dict[file_name] = json.load(file)
            except json.decoder.JSONDecodeError:
                searchs_dict[file_name] = {}

    return searchs_dict


def test_missing_searchs():
    all_searchs_items = get_search_from_dir('./search_files', 'a')
    ext_searchs_dict = get_articles_from_dir('./extracted_articles')

    articles_less_searchs = {}
    for inp_search_name, searchs in all_searchs_items:
        ext_searchs = ext_searchs_dict.get(inp_search_name)
        articles_less_searchs[inp_search_name] = []
        for search in searchs:
            ext_articles = ext_searchs.get(search, [])
            if len(ext_articles) <= 20:
                articles_less_searchs[inp_search_name].append(search)
        print(inp_search_name, len(articles_less_searchs[inp_search_name]))

    total_lose = sum([len(i) for i in articles_less_searchs.values()])
    print(total_lose)
    for search_name, searchs in articles_less_searchs.items():
        with open(os.path.join('./missing_searchs', search_name+'.txt'), 'a') as file:
            file.writelines([search+'\n' for search in searchs])

if __name__ == '__main__':
    logger = logging.getLogger('fileLogger')
    debug = True
    logger.disabled = not debug

    async_slice_size = 700
    search_mode = 'a'  # a: article, s: search, t: testing extracted articles :)
    # f: free (just search accesible for every one), l: limited (just those accesible for registered), a: all
    free_or_limited_search = 'f'

    if search_mode == 'a':
        ext_searchs_dict = get_articles_from_dir('./extracted_articles')
        asyncio.run(start_articles_parse(ext_searchs_dict))
    elif search_mode == 's':
        search_items = get_search_from_dir(
            './search_files', free_or_limited_search)
        asyncio.run(start_searchs_parse(search_items))
    elif search_mode == 't':
        test_missing_searchs()

#TODO: add test for knowing how many article got
#TODO: check database for authors withc not in article_authoers
#TODO#TODO:#TODO#TODO:#TODO#TODO: { fix error of network connection in article and search scraption } #TODO#TODO:#TODO#TODO:#TODO#TODO:#TODO#TODO:
#TODO do a better already exsit search of article skiption (for search: get count of article in db and if is near the count of article in file skip it)