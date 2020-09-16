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

def soup_maker (url, headers={}):
    try:
        content = requests.get(url, headers=headers).content
    except requests.exceptions.ConnectionError:
        raise(requests.exceptions.ConnectionError("[soup_maker] couldn't make a connection"))
    soup = bs(content, 'html.parser')
    return soup


class Find():
    def __init__(self, soup):
        self.soup = soup
    
    def xpath(self, xpath):
        raise(Exception('Not implimented yet'))

    def find_get(self, selector, get_attr):
        selected = self.soup.select_one(selector)
        return selected
    
    def select_one(self, selector):
        element = self.soup
        for num, selector_i in enumerate(selector.split('>')):
            element = element.select_one(selector_i)
            if element:
                print('[find]', num, selector_i, element.text)
            else :
                print('[find]', num, selector_i, 'element is none')
        return element
    
    def get_urls(self, include=[], _debug=False):
        #TODO include list shoud be regex
        res_urls = []
        link_divs = self.soup.find_all('a')
        
        for div in link_divs:
            href = div.get('href')
            if href and all([i in href for i in include]):
                res_urls.append(href)
                
        return res_urls

    def get_texts(self, include=[], _debug=False):
        raise(Exception('Not implimented yet'))

class Url():
    def __init__(self, url):
        self.url = url
        self._response_headers = None
        self._response = None
        self._content = None

        url_parts = urlparse(url)
        _query = frozenset(parse_qsl(url_parts.query))
        _path = unquote_plus(url_parts.path)
        self.url_parts = url_parts._replace(query=_query, path=_path)
    
    def is_downloadable(self):
        """
        Does the url contain a downloadable resource
        """
        h = requests.head(self.url, allow_redirects=True)
        header = h.headers
        content_type = header.get('content-type')
        if 'text' in content_type.lower():
            return False
        if 'html' in content_type.lower():
            return False
        return True

    @property
    def reponse(self):
        if not self._response:
            self._response = requests.get(self.url)
        return self._response

    def _get_filename_from_cd(cd):
        """
        Get filename from content-disposition
        """
        if not cd:
            return None
        fname = re.findall('filename=(.+)', cd)
        if len(fname) == 0:
            return None
        return fname[0]

    def __str__(self) -> str:
        return self.url

    def __eq__(self, other):
        #Checks netloc and path equality
        if not isinstance(other, Page):
            other = Url(other)
            
        return self.url_parts[1:3] == other.url_parts[1:3]

class Page(Find, Url):
    def __init__(self, url, headers={}, do_soup=False):
        super(Page, self).__init__(url)
        self.seen_count = 0
        self.text = ''
        if do_soup:
            self._soup = soup_maker(url, headers=headers)
    def __hash__(self):
        return hash(self.url.url_parts[1:3])

    #TODO this func should be replaced with find_get wich find an xpath or css path element and get and 
    def get_urls (self, include=[]):
        self.make_soup()
        res_urls = []
        link_divs = self.soup.find_all('a')

        for div in link_divs:
            href = div.get('href')
            if href and all([i in href for i in include]):
                res_urls.append(urljoin(self.url, href))

        return res_urls
    
    @property
    def soup(self):
        try :
            self.__getattribute__('_soup')
        except AttributeError :
            self._soup = soup_maker(self.url)
        return self._soup
    
    @soup.setter
    def soup(self, soup):
        self._soup = soup
            
    @soup.deleter
    def soup(self):
        del self._soup

class Article(Page):
    def __inti__(self, url):
        super(Article, self).__init__(url)
        self.pii = self.get_pii()
        self.bibtex = self.export_bibtext()
    
    def export_bibtex(self):
        bibtex_url = Url(f'https://www.sciencedirect.com/sdfe/arp/cite?pii={self.pii}&format=text/x-bibtex&wi')

        with open(f'articles/{self.pii}.bib', 'ab') as f:
            f.write
        
        return bibtex_url
    
    @property
    def authors(self, parameter_list):
        if not self._authors:
            
        return self._authors

class Seen_table():
    #TODO conflict betwine checking hash of page instance to check sameness
    def __init__(self):
        self.hash_table = {}
    def add_url(self, url):
        print('[seen_table] (add_url)')
        if self.is_in(url):
            return 'Already in'
        
        if not isinstance(url, Page):
            url = Page(url)
            
        self.hash_table[hash(url)] = url
        return "OK"
        
    def get_page(self, hash_url):
        return self.hash_table[hash_url]
        
    def is_in(self, url):
        print('[Seen_table] (is_in)')
        if not isinstance(url, Page):
            url = Page(url)
        #print(f'(Seen_table) [in_in] <{hash(url) in self.hash_table.keys()}> {url.url}')
        return hash(url) in self.hash_table.keys()


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



###### SEARCH PAGE TEST
search_url = 'https://www.sciencedirect.com/search?date={}&affiliations={}&show={}&sortBy=date'
article_history = {} 
author_history = {}
start_year = 2010
end_year = 2020

year = '2018-2020'
affiliations = 'iran'
show_count = 25

search_page = Page(search_url.format(year, affiliations, show_count), do_soup=True, headers=headers)

for i in range(show_count):
    article = search_page.select_one(f'li.ResultItem:nth-child({i+1}) > div:nth-child(1) > div:nth-child(1) > h2:nth-child(2) > span:nth-child(1) > a:nth-child(1)')
    print(i+1, end=' == ')
    if article :
        article_history[article.get('href')] = [] #authors add here
        print("article_added")
    else:
        print(type(article), article)


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
