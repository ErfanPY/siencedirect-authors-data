import asyncio
import json
import logging
import os
from urllib.parse import parse_qsl, urlparse

import aiohttp
from bs4 import BeautifulSoup as bs

from get_sd_ou.classUtil import Article, SearchPage
from get_sd_ou.databaseUtil import (connect_search_article, get_all_search, get_article, get_search, get_search_articles, init_db,
                                     insert_article_data, insert_search, update_article)


async def get_soup(session, url):
    async with session.get(url) as response:
        try:
            return await response.read()
        except Exception as e:
            return e


def filter_search(search_url, free_or_limited_search):
    offset = int(dict(parse_qsl(urlparse(search_url).query)).get('offset', 0))
    if free_or_limited_search == 'f':
        return offset < 1000
    elif free_or_limited_search == 'l':
        return offset >= 1000
    else:
        return 1


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


async def start_searchs_parse(search_items):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for search_name, search_urls in search_items:
            for search_url in search_urls:
                task = asyncio.ensure_future(
                    parse_search(session, search_url, search_name))
                tasks.append(task)
            search_result = await asyncio.gather(*tasks, return_exceptions=True)


async def parse_search(session, search_url, search_name):
    search_content = await get_soup(session, search_url)
    if not search_content:
        return search_url, []
    
    search_soup = bs(search_content, 'html.parser')
    SearchPage = SearchPage(url=search_url, soup_data=search_soup)

    articles = SearchPage.get_articles()
    articles = [i for i in articles if not'https://www.sciencedirect.com/science/article/pii/S' in i and not 'https://www.sciencedirect.com/science/article/pii/0' in i]
    [print(i) for i in articles]
    return search_name, search_url, articles
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


async def start_articles_parse(searchs_dict, skip_exist=True):
    for search_name, search_article_dict in searchs_dict.items():
        tasks = []
        for search_url, articles in search_article_dict.items():
            db_cnx = init_db()
            SearchPage = SearchPage(url=search_url)
            search_data = get_search(SearchPage.db_hash(), cnx=db_cnx)

            if search_data:
                search_id = search_data.get('search_id')
                if skip_exist and len(get_search_articles(search_url, cnx=db_cnx)) >= len(articles) - 5:
                    db_cnx.close()
                    continue

            else:
                search_id = insert_search(
                    search_hash=SearchPage.db_hash(), **SearchPage.search_kwargs, cnx=db_cnx)

            logger.debug('[parse_article|START] search_name: %s, search_url: %s  | articles count: %s',
                         search_name, search_url, len(articles))

            for article in articles:
                task = asyncio.ensure_future(parse_article(
                    article_url=article, search_url=search_url, search_id=search_id, db_cnx=db_cnx))
                tasks.append(task)

            result = await asyncio.gather(*tasks, return_exceptions=True)
            db_cnx.close()

            logger.debug('[start_parse_article|END] search_name: %s, search_url: %s  | articles count: %s',
                         search_name, search_url, len(articles))


async def parse_article(article_url, search_url, search_id, db_cnx, skip_exist=True):

    return_data = {"search_url": search_url, "article_url": article_url,
                   "search_id": search_id, "article_id": 0}
    
    logger.debug(log_formatter('parse_article|START', return_data))

    article_page = Article(url=article_url)
    article_data = get_article(article_page.pii, cnx=db_cnx)

    if article_data and skip_exist:
        return_data["article_id"] = article_data.get('article_id')
        return return_data

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
        update_article(
            **article_page.get_article_data(), cnx=db_cnx)

    return_data["article_id"] = article_id

    connect_search_article(search_id=search_id,
                           article_id=article_id, cnx=db_cnx)
    
    logger.debug(log_formatter('parse_article|END', return_data))
    return return_data


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


def not_parsed_searchs():
    ext_searchs_dict = get_articles_from_dir('./extracted_articles')
    # sum([len(j) for i, j in get_search_from_dir()]) # Count of all search url
    # sum([len(j) for i, j in ext_searchs_dict.items()]) # count of search with extracted article
    # len(get_all_search(cnx=db_cnx)) # count of in db searchs
    
    missed_searchs = {}

    db_cnx = init_db()
    db_searchs = get_all_search(cnx=db_cnx)
    hashs = [i.get('hash') for i in db_searchs]

    for inp_search_name, searchs in ext_searchs_dict.items():
        missed_searchs[inp_search_name] = []
    
        for search_url in searchs:
            SearchPage = SearchPage(url=search_url)
    
            if not SearchPage.db_hash() in hashs:
                print(search_url)
                missed_searchs[inp_search_name].append(search_url)

    total_lose = sum([len(i) for i in missed_searchs.values()])
    in_file_count = sum([len(j) for i, j in ext_searchs_dict.items()])
    
    print(f'[ {in_file_count} in input file // {len(hashs)} extracted in DB // {total_lose} lost search ]')
    
    for search_name, searchs in missed_searchs.items():
        with open(os.path.join('./missing_searchs', search_name+'.txt'), 'a') as file:
            file.writelines([search+'\n' for search in searchs])


def not_parsed_articles():
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
    debug = False
    logger = logging.getLogger('fileLogger')
    logging.getLogger("mainLogger").disabled = True
    
    async_slice_size = 700
    logger.disabled = not debug
    search_mode = 's'  # a: article, s: search, t: testing extracted articles
    free_or_limited_search = 'a' # f: free (just search accesible for every one), l: limited (just those accesible for registered), a: all
    
    if search_mode == 'a':
        ext_searchs_dict = get_articles_from_dir('./extracted_articles')
        asyncio.run(start_articles_parse(ext_searchs_dict))

    elif search_mode == 's':
        search_items = get_search_from_dir(
            './search_files', free_or_limited_search)
        asyncio.run(start_searchs_parse(search_items))

    elif search_mode == 'r':
        search_items = get_search_from_dir(
            './missing_searchs', free_or_limited_search)
        asyncio.run(start_searchs_parse(search_items))

    elif search_mode == 'ta':
        not_parsed_articles()

    elif search_mode == 'ts':
        not_parsed_searchs()

# TODO: check database for authors withc not in article_authoers
