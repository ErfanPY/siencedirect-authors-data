import asyncio

from aiohttp.helpers import current_task
import logging
import os
import time
from urllib.parse import parse_qsl, urlparse

import aiohttp
from bs4 import BeautifulSoup as bs

from get_sd_ou.class_util import Article, Search_page
from get_sd_ou.database_util import (connect_search_article, init_db,
                                     insert_article_data, insert_search)

async def get_soup(session, url):
    async with session.get(url) as response:
        return  bs(await response.read(), 'html.parser')
        
async def parse_article(session, article_url, search_url, db_cnx):
    article_content = await get_soup(session, article_url)
    article_soup = bs(article_content, 'html.parser')
    article_page = Article(url=article_url,soup_data=article_soup)
    article_id = insert_article_data(**article_page.get_article_data(), cnx=db_cnx)
    
    search_content = await get_soup(session, search_url)
    search_soup = bs(search_content, 'html.parser')
    search_page = Search_page(url=search_url, soup_data=search_soup)
    search_id = insert_search(search_hash=search_page.db_hash(), **search_page.search_kwargs, cnx=db_cnx)
    

    connect_search_article(search_id=search_id, article_id=article_id, cnx=db_cnx)


def filter_search(search_url, free_or_limited_search):
    offset = int(dict(parse_qsl(urlparse(search_url).query)).get('offset', 0))
    if free_or_limited_search == 'f':
        return offset <  1000
    elif free_or_limited_search == 'l':
        return offset >= 1000
    else :
        return 1

async def parse_search(session, queue):
    while True:
        search_name, search_url = await queue.get()
        logger.log(10001, 'startes ||'+search_name+' || '+search_url)
        search_soup = await get_soup(session, search_url)
        search_page = Search_page(url=search_url, soup_data=search_soup)
        articles = search_page.get_articles()
        with open(os.path.join('./extracted_articles', search_name), 'a') as file:
            file.write(search_url+'\n')
            file.writelines([i+'\n' for i in articles])
        logger.log(10001, 'done ||'+search_name+' || '+search_url)
        queue.task_done()
    
def get_searchs_from_dir(dir_path):
    file_list = [os.path.join(dir_path, file) for file in os.listdir(dir_path)]
    search_items = []
    for file_path in file_list:
        search_name = file_path.split('\\')[-1].split('.')[0]
        
        with open(file_path) as file:
            search_items += [(search_name, line.strip()) for line in file.readlines() if line.strip()]
    search_items = list(filter(lambda item: filter_search(item[1], free_or_limited_search), search_items))

    return search_items


async def start_searchs_parse(free_or_limited_search='a'):
    search_urls = get_searchs_from_dir('./search_files')
    queue = asyncio.Queue()
    workers_count = 10

    for search_name, search_url in search_urls:
            queue.put_nowait((search_name, search_url))
    
    tasks = []
    async with aiohttp.ClientSession() as session:
        for _ in range(workers_count):
            task = asyncio.ensure_future(parse_search(session, queue))
            tasks.append(task)

        started_at = time.monotonic()
        await queue.join()
    total_slept_for = time.monotonic() - started_at

    for task in tasks:
        task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)

    print(f'{workers_count} workers Got {len(search_urls)} search in {total_slept_for:.2f} seconds')

def get_articles_from_dir(dir_path):
    file_names = os.listdir(dir_path)
    file_list = [os.path.join(dir_path, file) for file in file_names]
    
    search_article_items = []
    for file_path in file_list:
        with open(file_path) as file:
            lines = [line.strip() for line in file.readlines() if line.strip()]
            current_search = ''
            for line in lines:
                if 'search' in line:
                    current_search = line
                elif 'article' in line:
                    search_article_items.append((current_search, line))

    return search_article_items
    
    for i in range(0, len(search_article_items), async_slice_size):
        article_slice_items = search_article_items[i:i+async_slice_size]

def test_extraction():
    extracted_search = {}
    

    for file_path in os.listdir('./extracted_articles'):
        with open(os.path.join('./extracted_articles', file_path)) as file:
            lines = [line.strip() for line in file.readlines() if line.strip()]
            current_search = ''
            for line in lines :
                if 'search' in line:
                    current_search = line
                    extracted_search[current_search] = []
                elif 'article' in line:
                    extracted_search[current_search].append(line)

    for file_path in os.listdir('./search_files'):
        with open(os.path.join('./search_files', file_path)) as file:
            lines = [line.strip() for line in file.readlines() if line.strip()]
            for line in lines :
    
                search_articles = extracted_search.get(line, [])
                if len(search_articles) < 100 :
                    print(file_path, len(search_articles), line)
                    with open(os.path.join('./missing_searchs',file_path), 'a') as file:
                        file.write(line+'\n')


if __name__ == '__main__':
    logger = logging.getLogger('mainLogger')
    logger.setLevel(10000)
    debug = True
    logger.disabled = not debug

    async_slice_size = 700
    search_mode = 's' # a: article, s: search, t: testing extracted articles :)
    free_or_limited_search = 'f' # f: free (just search accesible for every one), l: limited (just those accesible for registered), a: all
    
    if search_mode == 'a':
        asyncio.run(start_articles_parse(), debug=debug)
    elif search_mode == 's' :
        asyncio.run(start_searchs_parse(free_or_limited_search), debug=debug)
    elif search_mode == 't':
        test_extraction()
