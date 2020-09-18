class Url():
    def __init__(self, **kargs):
        self.url = kargs['url']
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

class Page(Find, Url):
    def __init__(self, url, headers={}, do_soup=False):
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
            pass
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
