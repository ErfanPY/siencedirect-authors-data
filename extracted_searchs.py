import asyncio
import logging
import os
import time

import aiohttp
from bs4 import BeautifulSoup as bs

from get_sd_ou.class_util import Article, Search_page


async def get_soup(session, url):
    async with session.get(url) as response:
        return  await response.read()

async def parse_search(session, search_url, search_article_dict):
    search_content = await get_soup(session, search_url)
    search_soup = bs(search_content, 'html.parser')
    search_page = Search_page(url=search_url, soup_data=search_soup)
    
    arts = search_page.get_articles()
    print(time.time(), ' || ', search_url, ' || ', len(arts))

async def start_searchs_parse(file_list, search_article_dict):
    async with aiohttp.ClientSession() as session:
        for file_path in file_list:
            with open(file_path) as file:
                # db_cnx = init_db()
                tasks = []
                for search_url in [i.strip() for i in file.readlines() if i and not i == ' ' and not i == '\n']:
                    task = asyncio.ensure_future(parse_search(session, search_url, search_article_dict=search_article_dict))
                    tasks.append(task)
                await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == '__main__':
    logger = logging.getLogger('mainLogger')
    logger.setLevel(1000)
    file_list = [os.path.join('./search_files', file) for file in os.listdir('./search_files')]
    search_article_dict = {}
    asyncio.run(start_searchs_parse(file_list, search_article_dict))
