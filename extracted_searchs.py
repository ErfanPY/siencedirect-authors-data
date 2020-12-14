import asyncio
import logging
import os
import time

import aiohttp
from bs4 import BeautifulSoup as bs

from get_sd_ou.class_util import Article, Search_page
from get_sd_ou.database_util import (connect_search_article, init_db,
                                     insert_article_data, insert_search)

logger = logging.getLogger('mainLogger')
# logger.disabled = True

async def get_soup(session, url):
    async with session.get(url) as response:
        return  await response.read()
        
async def parse_article(session, search_id, article_url, db_cnx):
    article_content = await get_soup(session, article_url)
    article_soup = bs(article_content, 'html.parser')
    article_page = Article(url=article_url,soup_data=article_soup)
    article_id = insert_article_data(**article_page.get_article_data(), cnx=db_cnx)
    connect_search_article(search_id=search_id, article_id=article_id, cnx=db_cnx)

    pass

async def parse_search(session, search_url, db_cnx, search_article_dict):
    search_task_list = []
    search_content = await get_soup(session, search_url)
    search_soup = bs(search_content, 'html.parser')
    search_page = Search_page(url=search_url, soup_data=search_soup)
    
    search_id = insert_search(search_hash=search_page.db_hash(), **search_page.search_kwargs, cnx=db_cnx)

    search_article_dict[search_id] = []

    for article_url in search_page.articles():
        search_article_dict[search_id].append(article_url) 
        # search_task_list.append(asyncio.ensure_future(parse_article(session=session, search_id=search_id, article_url=article_url, db_cnx=db_cnx)))
    # await asyncio.gather(*search_task_list, return_exceptions=True)

async def start_searchs_parse(file_list, search_article_dict):
    async with aiohttp.ClientSession() as session:
        for file_path in file_list:
            with open(file_path) as file:
                db_cnx = init_db()
                tasks = []
                for search_url in [i.strip() for i in file.readlines() if i and not i == ' ' and not i == '\n']:
                    task = asyncio.ensure_future(parse_search(session, search_url, db_cnx, search_article_dict=search_article_dict))
                    tasks.append(task)
                await asyncio.gather(*tasks, return_exceptions=True)


async def start_articles_parse(search_article_dict):
    async with aiohttp.ClientSession() as session:
        for سثشقز in file_list:
            with open(file_path) as file:
                db_cnx = init_db()
                tasks = []
                for search_url in [i.strip() for i in file.readlines() if i and not i == ' ' and not i == '\n']:
                    task = asyncio.ensure_future(parse_search(session, search_url, db_cnx, search_article_dict=search_article_dict))
                    tasks.append(task)
                await asyncio.gather(*tasks, return_exceptions=True)

file_list = [os.path.join('./search_files', file) for file in os.listdir('./search_files')]
search_article_dict = {}
asyncio.run(start_searchs_parse(file_list, search_article_dict))

# async def download_all_sites(sites):
#     async with aiohttp.ClientSession() as session:
#         tasks = []
#         for i in range(0, len(sites), 1000):    
#             for url in sites[i:i+1000]:
#                 task = asyncio.ensure_future(download_site(session, url))
#                 tasks.append(task)
#         await asyncio.gather(*tasks, return_exceptions=True)

# if __name__ == "__main__":
#     start_time = time.time()
#     asyncio.run(download_all_sites(sites))
#     duration = time.time() - start_time
#     print(f"Downloaded {len(sites)} sites in {duration} seconds")
