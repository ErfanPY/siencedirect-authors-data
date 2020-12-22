import asyncio
import json
import logging
import os
import time
from urllib.parse import parse_qsl, urlparse

import aiohttp
from bs4 import BeautifulSoup as bs

from get_sd_ou.class_util import Article, Search_page


async def get_soup(session, url):
    async with session.get(url) as response:
        try:
            return  await response.read()
        except:
            return None

async def parse_search(session, search_url):
    search_content = await get_soup(session, search_url)
    if not search_content:
        return search_url, []
    search_soup = bs(search_content, 'html.parser')
    search_page = Search_page(url=search_url, soup_data=search_soup)
    
    articles = search_page.get_articles()
    print(time.time(), ' || ', search_url, ' || ', len(articles))
    return search_url, articles

def filter_search(search_url, free_or_limited_search):
    offset = int(dict(parse_qsl(urlparse(search_url).query)).get('offset', 0))
    if free_or_limited_search == 'f':
        return offset <  1000
    elif free_or_limited_search == 'l':
        return offset >= 1000
    else :
        return 1

async def start_searchs_parse(search_items):
    async with aiohttp.ClientSession() as session:
            tasks = []
            for search_name, search_urls in search_items:
                for search_url in search_urls:
                    task = asyncio.ensure_future(parse_search(session, search_url))
                    tasks.append(task)
            search_result = await asyncio.gather(*tasks, return_exceptions=True)
        
            with open(os.path.join('./extracted_articles', search_name+".json"), 'a+') as file:
                try :
                    search_dict = json.load(file)
                except json.decoder.JSONDecodeError:
                    search_dict = {}
                for search_url, articles in search_result:

                    search_dict[search_url] = list(set(search_dict.get(search_url, []) + articles))
                    json.dump(search_dict, file)
                    logger.log(10001, f'{"DONE": >5} || {search_name} : {len(articles)} || {search_url}')
            
def is_in_extracted(search_name, url):
    with open(os.path.join('./extracted_articles', search_name+".json"), 'a+') as file:
        try :
            ext_search_dict = json.load(file)
        except json.decoder.JSONDecodeError:
            ext_search_dict = {}
        return len(ext_search_dict.get(url, [])) >= 10



def get_search_from_dir(dir):
    free_or_limited_search = 'f'
    search_dict = {}
    for file_name in os.listdir(dir):
        search_name = file_name.split('.')[0]
        file_path = os.path.join(dir, file_name)
        
        with open(file_path) as file:
            search_lines = [i.strip() for i in file.readlines() if i.strip()]
            search_urls = list(filter(lambda url: filter_search(url, free_or_limited_search) and \
                                                not is_in_extracted(search_name, url),
                                                search_lines))
            search_dict[search_name] = search_urls
    return search_dict.items()




if __name__ == '__main__':
    logger = logging.getLogger('mainLogger')
    logger.setLevel(1000)
    search_items = get_search_from_dir('./search_files')          
                
    asyncio.run(start_searchs_parse(search_items))
