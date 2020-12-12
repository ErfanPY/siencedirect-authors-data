from bs4 import BeautifulSoup as bs
import time
import logging
from get_sd_ou.class_util import Article, Search_page
import asyncio
import aiohttp


logger = logging.getLogger('mainLogger')
logger.disabled = True

sites = []

with open('Blockchain.txt') as file:
    for url in file.readlines():
        s_len = len(sites)
        [sites.append(article) for article in Search_page(url.strip()).get_articles()]

sites= list(set(sites))
articles_data = {}

async def download_site(session, url):
    async with session.get(url) as response:
        content = await response.read()
        soup = bs(content, 'html.parser')
        datas = Article(url, soup_data=soup).get_article_data()
        articles_data[url] = datas
        
async def download_all_sites(sites):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in range(0, len(sites), 1000):    
            for url in sites[i:i+1000]:
                task = asyncio.ensure_future(download_site(session, url))
                tasks.append(task)
        await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(download_all_sites(sites))
    duration = time.time() - start_time
    print(f"Downloaded {len(sites)} sites in {duration} seconds")


# print('MEEEEEE')