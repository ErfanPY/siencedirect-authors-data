#%%
import logging
import queue
import re
from re import search
import threading
from urllib.parse import parse_qsl, unquote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup as bs

from get_sd_ou.databse_util import get_article_authors
from get_sd_ou.class_util import Article, Search_page


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


main_queue = queue.Queue()

def soup_maker (url, headers={}):
    try:
        content = requests.get(url, headers=headers).content
    except requests.exceptions.ConnectionError:
        raise(requests.exceptions.ConnectionError("[soup_maker] couldn't make a connection"))
    soup = bs(content, 'html.parser')
    return soup

def main():

    ###### SEARCH PAGE TEST
    base_url = 'https://www.sciencedirect.com/'
    search_url = 'https://www.sciencedirect.com/search?date={}&show={}&sortBy=date'
    article_history = {} 
    author_history = {}
    start_year = 2010
    end_year = 2020

    year = '2020'
    show_count = 25

#%%-
def worker():
    while True:
        if main_queue.empty():
            next_search = Search_page(next(search_url_gen))
            next_articles = next_search.get_articles()
            [main_queue.put(next_article) for next_article in next_articles]

        article_url = main_queue.get()
        article = Article(article_url, headers)
        article_data = article.get_data()
        dbutil.insert_article_data(article_data)
        
        main_queue.task_done()
    
[threading.Thread(target=worker).start() for _ in range(2)]

# block until all tasks are done
main_queue.join()
print('All work completed')
