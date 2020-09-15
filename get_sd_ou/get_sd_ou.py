
import requests
from bs4 import BeautifulSoup as bs
from urllib.parse import urlparse, parse_qsl, unquote_plus, urljoin

search_url = 'https://www.sciencedirect.com/search?qs=art&date={}'
headers = {
        'Accept' : 'application/json',
        'user-agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/80.0'
        }
history = {}
start_year = 2010
end_year = 2020

for year in range(end_year, start_year, -1):
    search = requests.get(search_url.format(year), headers=headers)
    soup = bs(search.content)
    soup.find_all('a', )


class Find():
    def __init__(self, soup):
        self.soup = soup
    
    def xpath(self, xpath):
        raise(Exception('Not implimented yet'))

    def find_get(self, selector, get_attr):
        selected = self.soup.select_one(selector)
        return selected
    
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

class Page(Find):
    def __init__(self, url, headers={}, do_soup=False):
        self.url = Url(url)
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
                res_urls.append(href)

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

class Url():
    def __init__(self, url):
        self.url = url
        
        url_parts = urlparse(url)
        _query = frozenset(parse_qsl(url_parts.query))
        _path = unquote_plus(url_parts.path)
        self.url_parts = url_parts._replace(query=_query, path=_path)
        
    def __eq__(self, other):
        #Checks netloc and path equality
        if not isinstance(other, Page):
            other = Url(other)
            
        return self.url_parts[1:3] == other.url_parts[1:3]

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

def soup_maker (url, headers={}):
    try:
        content = requests.get(url, headers=headers).content
    except requests.exceptions.ConnectionError:
        raise(requests.exceptions.ConnectionError("[soup_maker] couldn't make a connection"))
    soup = bs(content, 'html.parser')
    return soup

def add_queue(url):
    #join the url -> Page the url -> add to queue & add to seen_table
    url = urljoin(base_url, url)
    page = Page(url)
    if seen_table.is_in(page):
        return
    seen_table.add_url(page)
    main_queue.put(page)
