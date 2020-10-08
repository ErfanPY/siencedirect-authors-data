import re
import json
import logging 
import requests
from bs4 import BeautifulSoup as bs
from urllib.parse import urlparse, urljoin, parse_qsl, unquote_plus

logger = logging.getLogger('mainLogger')

class Url():
    def __init__(self, url, headers={}, **kwargs):
        logger.debug('[ Url ] __init__ | url: %s', url)
        self.url = url
        self._response_headers = None
        #self._response = None #this would be created when requested
        #self._content = None #this would be created when requested

        if not headers:
            self.headers = {
                    'Accept' : 'application/json, text/plain, */*',
                    'user-agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/80.0'
                    }

        url_parts = urlparse(self.url)
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
    def response(self):
        try :
            self.__getattribute__('_response')
            logger.debug('[ Url ] response exist')
            return self._response
        except AttributeError :
            logger.debug('[ Url ] getting url response | url: %s', self.url)
            self._response = requests.get(self.url, headers=self.headers)
        return self._response

    def _get_filename_from_cd(self, cd):
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

class Find():
    def __init__(self, soup, **kwargs):
        logger.debug('[ Find ] __init__')
        self.soup = soup
    
    def xpath(self, xpath):
        raise(Exception('Not implimented yet'))

    def find_get(self, selector, get_attr):
        selected = self.soup.select_one(selector)
        return selected
    
    def select_one(self, selector):
        """
        element = self.soup
        for num, selector_i in enumerate(selector.split('>')):
            element = element.select_one(selector_i)
            if element.content:
                logger.debug('[ Find ] selector_num:({}) selector:({}) element:({})'.format(num, selector_i, element.content[:20]))
            else :
                logger.debug('[ Find ] selector_num:({}) selector:({}) element is none'.format(num, selector_i))
                return None
        return element
        """
        raise NotImplementedError
    
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

class Page(Url, Find):
    def __init__(self, url, do_soup=False, **kwargs):
        logger.debug('[ Page ] __init__ | url: %s', url)
        super().__init__(url=url, soup=None)

        self.seen_count = 0
        self.text = ''
        if do_soup:
            self._soup = self._soup_maker()
    def __hash__(self):
        return hash(self.url.url_parts[1:3])

    #TODO this func should be replaced with find_get wich find an xpath or css path element and get and 
    def get_urls (self, include=[]):
        res_urls = []
        link_divs = self.soup.find_all('a')

        for div in link_divs:
            href = div.get('href')
            if href and all([i in href for i in include]):
                res_urls.append(urljoin(self.url, href))

        return res_urls
    
    def _soup_maker(self):
        try:
            content = self.response.content
        except requests.exceptions.ConnectionError:
            raise(requests.exceptions.ConnectionError("[Page] [soup_maker] couldn't make a connection"))
        soup = bs(content, 'html.parser')
        return soup

    @property
    def soup(self):
        try :
            self.__getattribute__('_soup')
            if self._soup :
                logger.debug('[ Page ] soup exist | url: %s', self.url)
                return self._soup
        except AttributeError :
            pass    
        self._soup = self._soup_maker()
        logger.debug(f'[ Page ] soup made | len_soup: {len(str(self._soup))}')
        return self._soup
    
    @soup.setter
    def soup(self, soup):
        self._soup = soup
            
    @soup.deleter
    def soup(self):
        del self._soup

class Author(dict):
    def __init__(self, first_name, last_name, id='',email='', affiliation='', is_coresponde=False):
        logger.debug('[ Author ] __init__ | name: %s', first_name + last_name)
        self['first_name'] = first_name
        self['last_name'] = last_name
        self['id'] = id
        self['email'] = email
        self['affiliation'] = affiliation
        self['is_coresponde'] = is_coresponde
    
    def __str__(self) -> str:
        return self.first_name + self.last_name
    
    def __getattr__(self, key):
        return(self[key])


class Article(Page):
    def __init__(self, url, do_bibtex=False, *args, **kwargs):
        self.url = url
        self.pii = self.get_pii()
        logger.debug('[ Article ] __init__ | pii: %s', self.pii)
        super().__init__(url, *args, **kwargs)
        self.bibtex = None
        if do_bibtex: self.bibtex = self.export_bibtex()
        self._authors = []

    def get_pii(self):
        return self.url.split('/')[-1].replace('#!', '')

    def get_article_data(self, *needed_data):
        """ this is the main function of article it collect all data we need from an article (needed data is spesified from input) 
        it get authors name and email and affiliation from article and mendely link if exist
        """
        data = {'pii':self.pii, 'authors':self.authors}

        return data
    
    def export_bibtex(self, download=False):
        bibtex_url = Url(f'https://www.sciencedirect.com/sdfe/arp/cite?pii={self.pii}&format=text/x-bibtex&wi')

        if not download : 
            return {'bibtex_url':bibtex_url}
        bibtex_path = f'articles/{self.pii}.bib'
        with open(bibtex_path, 'ab') as f:
            f.write(requests.get(bibtex_url, headers=self.headers))
        return {'bibtex_url':bibtex_url, 'bibtex_path':bibtex_path}

    def _author_icons(self, tag_a):
        is_coresponde = bool(tag_a.select('.icon-person'))
        has_email = bool(tag_a.select('.icon-envelope'))
        return {'has_email':has_email,'is_coresponde':is_coresponde}

    def _author_from_json(self):
        json_element = self.soup.find_all('script', {'type':"application/json"})[0].contents[0]
        json_data = json.loads(str(json_element))

        authors_res = {}
        authors_list_json = []
        authors_groups_list_json =  json_data['authors']['content']
        authors_groups_list_json = list(filter(lambda dict: dict['#name']=='author-group', authors_groups_list_json))
        for group in authors_groups_list_json:
            group_aff = list(filter(lambda dict: dict['#name']=='affiliation', group['$$']))[0]['$$'][0]['_']
            group_aff_country = group_aff.split(',')[-1].strip()
            group_authors = list(filter(lambda dict: dict['#name']=='author', group['$$']))
            [authors_list_json.append((group_author, group_aff_country)) for group_author in group_authors]

        for index, (author_json, affiliation_country) in enumerate(authors_list_json):
            first_name = author_json['$$'][0]['_']
            last_name = author_json['$$'][1]['_']
            email_check = list(filter(lambda dic: dic['#name'] == 'e-address', author_json['$$']))
            email = None if not email_check else email_check[0]['_']
            """
            splited_aff = list(json_data['authors']['affiliations'].items())[0][1]['$$']
            affiliation_text = list(filter(lambda dic: dic['#name'] == 'textfn', splited_aff))[0]['_']
            affiliation_country = affiliation_text.split(',')[-1]
            """
            authors_res[index] = {'first_name':first_name, 'last_name':last_name, 'email':email, 'affiliation':affiliation_country}
            
        return authors_res

    @property
    def authors(self):
        if not self._authors:
            elements = self.soup.select_one('#author-group').find_all('a')
            authors_data = self._author_from_json()
            for index, author_element in enumerate(elements):
                icons = self._author_icons(author_element)
                authors_data[index]['is_coresponde'] = icons['is_coresponde']
            authors_objects = [Author(**author_data) for author_data in authors_data.values()]
            self._authors = authors_objects
            logger.debug('[ Article ] authors: %s', self._authors)
        return self._authors

class Search_page (Page):
    def __init__(self, url):
        logger.debug('[ Search_page ] __init__ | url: %s', url)
        super().__init__(url)
        self.url = url
        self.query = dict(self.url_parts.query)
        self.year = self.query['date']
    
    def get_articles(self):
        logger.debug('[ Search_page ] getting articles | year: %s', self.year)
        search_result = self.soup.find_all('a')
        articles = []
        for article in search_result :
            if article.get('href'):
                article_link = article.get('href')
                if 'pii' in article_link and not 'pdf' in article_link:
                    articles.append(urljoin('https://'+self.url_parts.netloc, article_link))
                    logger.debug('[ Search_page ] one article added | year: %s', self.year)
        logger.debug('[ Search_page ] all articels got | year: %s', self.year)
        return articles
    
    @property
    def pages_count(self):
        return self.soup.select_one('#srp-pagination > li:nth-child(1)')

    def next_page(self):
        next_url = self.soup.select_one('li.next-link > a')
        try:
            href = next_url.get('href')
            return urljoin(self.url_parts.netloc, href)
        except AttributeError:
            return None
    
    def export_bibtex(self, file):
        raise NotImplementedError

class Seen_table():
    #TODO conflict betwine checking hash of page instance to check sameness
    def __init__(self):
        self.hash_table = {}
    def add_url(self, url):
        print('[ seen_table ] (add_url)')
        if self.is_in(url):
            return 'Already in'
        
        if not isinstance(url, Page):
            url = Page(url)
            
        self.hash_table[hash(url)] = url
        return "OK"
        
    def get_page(self, hash_url):
        return self.hash_table[hash_url]
        
    def is_in(self, url):
        print('[ Seen_table ] (is_in)')
        if not isinstance(url, Page):
            url = Page(url)
        #print(f'(Seen_table) [in_in] <{hash(url) in self.hash_table.keys()}> {url.url}')
        return hash(url) in self.hash_table.keys()