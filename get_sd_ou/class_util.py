#%%
class Url():
    def __init__(self, url, *args, **kwargs):
        logger.debug('[Url] initiated')
        self.url = url
        self._response_headers = None
        self._response = None
        self._content = None

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

class Find():
    def __init__(self, *args, **kwargs):
        logger.debug('[Find] initiated')
        self.soup = kwargs['soup']
    
    def xpath(self, xpath):
        raise(Exception('Not implimented yet'))

    def find_get(self, selector, get_attr):
        selected = self.soup.select_one(selector)
        return selected
    
    def select_one(self, selector):
        element = self.soup
        for num, selector_i in enumerate(selector.split('>')):
            element = element.select_one(selector_i)
            if element.content:
                logger.debug('[Find] selector_num:({}) selector:({}) element:({})'.format(num, selector_i, element.content[:20]))
            else :
                logger.debug('[Find] selector_num:({}) selector:({}) element is none'.format(num, selector_i))
                return None
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


class Page(Find, Url):
    def __init__(self, url, headers={}, do_soup=False):
        logger.debug('[Page] initiated')
        super(Page, self).__init__(url=url, soup=None)
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

class Author():
    def __init__(self, name, email='', mendely='', scopus='', affiliation=''):
        logger.debug('[Author] initiated')
        self.name = name
        self.email = email
        self.mendely = mendely
        self.scopus = scopus
        self.affiliation = affiliation

class Article(Page):
    def __inti__(self, url, do_bibtex=False, *args, **kwargs):
        logger.debug('[Article] initiated')
        super().__init__(url, *args, **kwargs)
        self.pii = self.get_pii()
        self.bibtex = None
        if do_bibtex: self.bibtex = self.export_bibtex()
        self._authors = []

    def get_article_data(self, *needed_data):
        """ this is the main function of article it collect all data we need from an article (needed data is spesified from input) 
        it get authors name and email and affiliation from article and mendely link if exist
        """
        data = {'authors':self.authors, 
                }

        raise NotImplementedError
    
    def export_bibtex(self, download=False):
        bibtex_url = Url(f'https://www.sciencedirect.com/sdfe/arp/cite?pii={self.pii}&format=text/x-bibtex&wi')

        if not download : 
            return {'bibtex_url':bibtex_url}
        bibtex_path = f'articles/{self.pii}.bib'
        with open(bibtex_path, 'ab') as f:
            f.write(requests.get(bibtex_url, headers=self.headers))
        return {'bibtex_url':bibtex_url, 'bibtex_path':bibtex_path}
           
    @property
    def authors(self):
        if self.getattr('_authors'):
            authors_element = [element.find('span', {'class':'content'}) for element in self.soup.select_one('#author-group').find_all('a')]
            return authors_element
        return self._authors

print('test')
print('test')
#%%
class Search_page (Page):
    def __init__(self, url):
        logger.debug('[Search_page] initiated')
        super().__init__(url)
        self.url = url
        self._pages_count = -1
    
    def get_articles(self):
        search_result = self.soup.find_all('a')
        articles = []
        for article in search_result :
            if article.get('href'):
                article_link = article.get('href')
                if 'pii' in article_link and not 'pdf' in article_link:
                    articles.append(urljoin(base_url, article_link))
        return articles
    
    @property
    def pages_count(self):
        return self.soup.select_one('#srp-pagination > li:nth-child(1)')

    def next_search_url(self):
        next_url = self.soup.select_one('.pagination-link > a:nth-child(1)').get('href')
        return urljoin(base_url, next_url)
    
    def export_bibtex(self, file):
        raise NotImplementedError

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