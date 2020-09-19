#%%
from get_sd_ou.class_util import Page, Seen_table
from get_sd_ou import database_util as dbutil

import logging
import queue
import re
import threading
from urllib.parse import parse_qsl, unquote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup as bs


fh = logging.FileHandler('logs.log')
fh.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', handlers=[fh, ch])
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

headers = {
        'Accept' : 'application/json',
        'user-agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/80.0'
        }


seen_table = Seen_table()

main_queue = queue.Queue()


def add_queue(base_url, url):
    #join the url -> Page the url -> add to queue & add to seen_table
    url = urljoin(base_url, url)
    page = Page(url)
    if seen_table.is_in(page):
        return
    seen_table.add_url(page)
    main_queue.put(page)


def soup_maker (url, headers={}):
    try:
        content = requests.get(url, headers=headers).content
    except requests.exceptions.ConnectionError:
        raise(requests.exceptions.ConnectionError("[soup_maker] couldn't make a connection"))
    soup = bs(content, 'html.parser')
    return soup

###### SEARCH PAGE TEST
base_url = 'https://www.sciencedirect.com/'
search_url = 'https://www.sciencedirect.com/search?date={}&show={}&sortBy=date'
article_history = {} 
author_history = {}
start_year = 2010
end_year = 2020

year = '2020'
show_count = 25

search_page = Page(search_url.format(year, show_count), do_soup=True, headers=headers)

#%%
search_result = search_page.soup.find_all('a')
articles = []
for article in search_result :
    if article.get('href'):
        article_link = article.get('href')
        if 'pii' in article_link and not 'pdf' in article_link:
            articles.append(urljoin(base_url, article_link))

#%%
def worker():
    while True:
        page_inst = main_queue.get()
        #get all links and add them to main_queue
        #get all text and add to item.urls
    
            
        urls = page_inst.get_urls()
        for i_url in urls:
            add_queue(i_url)
                
        main_queue.task_done()
    
[threading.Thread(target=worker).start() for _ in range(2)]

# block until all tasks are done
main_queue.join()
print('All work completed')    

# %%
