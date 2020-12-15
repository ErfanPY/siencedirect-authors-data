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
        return  await response.read()
        
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

async def parse_search(session, search_url, search_name):
    search_content = await get_soup(session, search_url)
    search_soup = bs(search_content, 'html.parser')
    search_page = Search_page(url=search_url, soup_data=search_soup)
    
    with open(os.path.join('./extracted_articles', search_name), 'a') as file:
        file.write(search_url+'\n')
        file.writelines([i+'\n' for i in search_page.get_articles()])


async def start_searchs_parse(search_items):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for search_name, search_url in search_items:
            task = asyncio.ensure_future(parse_search(session, search_url, search_name))
            tasks.append(task)
        await asyncio.gather(*tasks, return_exceptions=True)

def start_start_serach(free_or_limited_search='a'):
    file_list = [os.path.join('./search_files', file) for file in os.listdir('./search_files')]
    search_items = []
    for file_path in file_list:
        search_name = file_path.split('\\')[-1].split('.')[0]
        with open(file_path) as file:
            lines = file.readlines()
            for line in lines :
                line = line.strip() if line and not line == ' ' and not line == '\n' else ''
                search_items.append((search_name, line))

    start_time = time.time()
    def filter_search(search_url, free_or_limited_search):
        offset = int(dict(parse_qsl(urlparse(search_url).query)).get('offset', 0))
        if free_or_limited_search == 'f':
            return offset <  1000
        elif free_or_limited_search == 'l':
            return offset >= 1000
        else :
            return 1
    
    search_items = list(filter(lambda item: filter_search(item[1], free_or_limited_search), search_items))

    for i in range(0, len(search_items), async_slice_size):
        search_slice_items = search_items[i:i+async_slice_size]
        
        slice_start_time = time.time()

        asyncio.run(start_searchs_parse(search_items=search_slice_items))

        slice_end_time = time.time()
        slice_duration = slice_end_time - slice_start_time

        print(f'Prosseced {async_slice_size} in {slice_duration} second')
        with open('result_time.txt', 'a') as f:
            f.write(f'Prosseced {async_slice_size} in {slice_duration} second')
    
    duration = time.time()-start_time
    with open('result_time.txt', 'a') as f:
            f.write(f'{len(search_items)} search link in {duration}s')
    input(f'{len(search_items)} search link in {duration}s')

async def start_articles_parse(search_article_items):
    async with aiohttp.ClientSession() as session:
        tasks = []
        db_cnx = init_db()
        for search_url, article_url in search_article_items:
            task = asyncio.ensure_future(parse_article(session=session, article_url=article_url, search_ur=search_url, db_cnx=db_cnx))
            tasks.append(task)
        await asyncio.gather(*tasks, return_exceptions=True)

def start_start_article():
    file_list = [os.path.join('./extracted_files', file) for file in os.listdir('./extracted_files')]
    search_article_items = []

    for file_path in file_list:
        with open(file_path) as file:
            lines = file.readlines()
            current_search = ''
            for line in lines :
                line = line.strip() if line and not line == ' ' and not line == '\n' else ''
                if 'search' in line:
                    current_search = line
                elif 'article' in line:
                    search_article_items.append((current_search, line))

    start_time = time.time()
    search_article_items = search_article_items.items()
    
    for i in range(0, len(search_article_items), async_slice_size):
        article_slice_items = search_article_items[i:i+async_slice_size]

        slice_start_time = time.time()
        asyncio.run(start_articles_parse(article_slice_items))

        slice_end_time = time.time()
        slice_duration = slice_end_time - slice_start_time

        print(f'Prosseced {async_slice_size} in {slice_duration} second')
        with open('result_time.txt', 'a') as f:
            f.write(f'Prosseced {async_slice_size} in {slice_duration} second')
    
    duration = time.time()-start_time
    with open('result_time.txt', 'a') as f:
            f.write(f'{len(search_article_items)} article link in {duration}s')
    print(f'{len(search_article_items)} article link in {duration}s')


if __name__ == '__main__':
    logger = logging.getLogger('mainLogger')
    # logger.disabled = True

    async_slice_size = 700
    article_or_search = 's' # a for article, s for search :)
    free_or_limited_search = 'f' # f for free (just search accesible for every one), l for limited (just those accesible for registered), a for all
    
    if article_or_search == 'a':
        start_start_article()
    elif article_or_search == 's' :
        start_start_serach(free_or_limited_search)

# async def download_all_sites(sites):
#     async with aiohttp.ClientSession() as session:
#         tasks = []
#         for i in range(0, len(sites), 1000):    
#             for url in sites[i:i+1000]:
#                 task = asyncio.ensure_future(download_site(session, url))
#                 tasks.append(task)
#         await asyncio.gather(*tasks, return_exceptions=True)
