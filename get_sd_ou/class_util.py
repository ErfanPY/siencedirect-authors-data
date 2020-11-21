from hashlib import sha1
import json
import logging
import re
from urllib.parse import parse_qsl, unquote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup as bs

logger = logging.getLogger('mainLogger')


class Url():
    def __init__(self, url, headers={}, **kwargs):
        logger.debug('[ Url ] __init__ | url: %s', url)
        self.url = url
        self._response_headers = None
        # self._response = None #this would be created when requested
        # self._content = None #this would be created when requested

        if not headers:
            self.headers = {
                'Accept': 'application/json, text/plain, */*',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/80.0'
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
        try:
            self.__getattribute__('_response')
            logger.debug('[ Url ] response exist')
            return self._response
        except AttributeError:
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
        # Checks netloc and path equality
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
        # TODO include list shoud be regex
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

    # TODO this func should be replaced with find_get wich find an xpath or css path element and get and
    def get_urls(self, include=[]):
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
            raise(requests.exceptions.ConnectionError(
                "[Page] [soup_maker] couldn't make a connection"))
        soup = bs(content, 'html.parser')
        return soup

    @property
    def soup(self):
        try:
            self.__getattribute__('_soup')
            if self._soup:
                logger.debug('[ Page ] soup exist | url: %s', self.url)
                return self._soup
        except AttributeError:
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
    def __init__(self, first_name, last_name, id='', email='',
                 affiliation='', is_coresponde=False, do_scopus=False):

        logger.debug('[ Author ] __init__ | name: %s', first_name + last_name)
        self['first_name'] = first_name
        self['last_name'] = last_name
        self['id'] = id
        self['email'] = email
        self['affiliation'] = affiliation
        self['is_coresponde'] = is_coresponde
        if do_scopus:
            self.get_scopus()

    def get_scopus(self):
        scopus_search = 'https://www.scopus.com/results/authorNamesList.uri?sort=count-f&src=al&sid=9d5d4784ba0ec31261499d113b0fc914&sot=al&sdt=al&sl=52&s=AUTHLASTNAME%28EQUALS%28{0}%29%29+AND+AUTHFIRST%28{1}%29&st1={0}&st2={1}&orcidId=&selectionPageSearch=anl&reselectAuthor=false&activeFlag=true&showDocument=false&resultsPerPage=20&offset=1&jtp=false&currentPage=1&previousSelectionCount=0&tooManySelections=false&previousResultCount=0&authSubject=LFSC&authSubject=HLSC&authSubject=PHSC&authSubject=SOSC&exactAuthorSearch=true&showFullList=false&authorPreferredName=&origin=searchauthorfreelookup&affiliationId=&txGid=2902d9dc14a46e0e513784d44e52bc5d'
        scopus_url = Page(scopus_search.format(
            self['last_name'], self['first_name']))
        inputs = scopus_url.soup.find('input', {'id': re.compile('auid_.*')})
        if not inputs:
            return None
        self['id'] = inputs.get('value')
        return f"https://www.scopus.com/authid/detail.uri?authorId={self['id']}"

    def __str__(self) -> str:
        return self.first_name + self.last_name

    def __getattr__(self, key):
        return(self[key])

class json_parse:
    def __init__(self, json_data):
        self.json_data = json_data
    
    def filter(self, filter_by):
        pass

class Article(Page):
    def __init__(self, url, do_bibtex=False, *args, **kwargs):
        self.url = url
        self.pii = self.get_pii()
        logger.debug('[ Article ] __init__ | pii: %s', self.pii)
        super().__init__(url, *args, **kwargs)
        self.bibtex = ''
        self.title = self.soup.select_one('.title-text').text
        if do_bibtex:
            self.bibtex = self.export_bibtex()

        self._authors = []

    def get_pii(self):
        return self.url.split('/')[-1].replace('#!', '')
    
    def get_article_data(self, *needed_data):
        """ this is the main function of article it collect all data we need from an article (needed data is spesified from input) 
        it get authors name and email and affiliation from article 
        """
        data = {'pii': self.pii, 'authors': self.authors, 'bibtex': self.bibtex, 'title':self.title}

        return data

    def export_bibtex(self):
        self.bibtex_url = Url(
            f'https://www.sciencedirect.com/sdfe/arp/cite?pii={self.pii}&format=text/x-bibtex&wi')

        self.bibtex_file_path = f'articles/{self.pii}.bib'
        with open(self.bibtex_file_path, 'ab') as f:
            f.write(requests.get(self.bibtex_url, headers=self.headers))
        return self.bibtex_url

    def _author_icons(self, tag_a):
        is_coresponde = bool(tag_a.select('.icon-person'))
        has_email = bool(tag_a.select('.icon-envelope'))
        return {'has_email': has_email, 'is_coresponde': is_coresponde}

    def _author_from_json(self):
        logger.debug('[class] [Article] getting authors from json')
        json_element = self.soup.find_all(
            'script', {'type': "application/json"})[0].contents[0]
        json_data = json.loads(str(json_element))

        authors_res = {}
        authors_list_json = []
        affiliations_list = re.findall(r'country[^\]\}]*"([^\]\}]*)"', json_element)
        if not affiliations_list :
            #TODO
            affiliations_list = [' ']
        authors_groups_list_json = json_data['authors']['content']
        authors_groups_list_json = list(
            filter(lambda dict: dict['#name'] == 'author-group', authors_groups_list_json))

        for group in authors_groups_list_json:  # in authors maybe some group which devides authors
                group_authors = list(
                    filter(lambda dict: dict['#name'] == 'author', group['$$']))
                [authors_list_json.append(group_author) for group_author in group_authors]
        _affiliations_data_dict = json_data['authors']['affiliations']
        for index, author_json in enumerate(authors_list_json):
            reference_list = list(filter(lambda dict: dict['#name'] == 'cross-ref', author_json['$$']))
            _affiliations_id_list = [ref['$']['refid'] for ref in reference_list]
            affiliation_country = affiliations_list[index % len(affiliations_list)]
            first_name = author_json['$$'][0]['_']
            try:
                last_name = author_json['$$'][1]['_']
            except KeyError:
                last_name = " "
            email_check = list(
                filter(lambda dic: dic['#name'] == 'e-address', author_json['$$']))
            try:
                email = None if not email_check else email_check[0]['_']
            except KeyError:
                email = email_check[0]['$$'][0]['_']
            """
            splited_aff = list(json_data['authors']['affiliations'].items())[0][1]['$$']
            affiliation_text = list(filter(lambda dic: dic['#name'] == 'textfn', splited_aff))[0]['_']
            affiliation_country = affiliation_text.split(',')[-1]
            """
            authors_res[index] = {'first_name': first_name, 'last_name': last_name,
                                  'email': email, 'affiliation': affiliation_country}
        return authors_res

    @property
    def authors(self):
        logger.debug('[class] [Article] getting authors')
        if not self._authors:
            elements = self.soup.select_one('#author-group').find_all('a')
            authors_data = self._author_from_json()
            for index, author_element in enumerate(elements):
                icons = self._author_icons(author_element)
                authors_data[index]['is_coresponde'] = icons['is_coresponde']
                logger.info('Author got, %s', authors_data[index])

            authors_objects = [Author(**author_data)
                               for author_data in authors_data.values()]
            self._authors = authors_objects
            logger.debug('[ Article ] authors: %s', self._authors)
        return self._authors
    
class Search_page (Page):
    def __init__(self, url='', show_per_page=100, start_offset=0, **search_kwargs):
        if not url:
            #url = f'https://www.sciencedirect.com/search?qs={title}&date={url}&authors={author}&affiliations={affiliation}&show={show_per_page}'
            url = 'https://www.sciencedirect.com/search?'
            for key, value in search_kwargs.items():
                if value:
                    url += '{}={}&'.format(key, value)
            url += 'show={}&'.format(show_per_page)
        logger.debug('[ Search_page ] __init__ | url: %s', url)
        super().__init__(url)

        self.url = url
        self.search_kwargs = search_kwargs
        self.query = dict(self.url_parts.query)
        self.show_per_page = show_per_page
        self.offset = self.query.get('offset', start_offset)

    def db_hash(self):
        search_kwargs = self.search_kwargs
        search_kwargs['offset'] = 0
        return sha1(json.dumps(search_kwargs, sort_keys=True, ensure_ascii=False).encode('utf-8')).hexdigest()

    def __bool__(self):
        return self.url != ''

    def get_articles(self):
        logger.debug('[ Search_page ] getting articles | url: %s', self.url)
        search_result = self.soup.find_all('a')
        articles = []
        for article in search_result:
            if article.get('href'):
                article_link = article.get('href')
                if 'pii' in article_link and not 'pdf' in article_link:
                    articles.append(
                        urljoin('https://'+self.url_parts.netloc, article_link))
                    logger.debug(
                        '[ Search_page ] one article added | url: %s', self.url)
        logger.debug('[ Search_page ] all articels got | url: %s', self.url)
        return articles

    @property
    def curent_page_num(self):
        if not hasattr(self, '_curent_page_num'):
            page_coursor_text = self.soup.select_one(
                '#srp-pagination > li:nth-child(1)').text
            if page_coursor_text == 'previous':
                page_coursor_text = self.soup.select_one('#srp-pagination > li:nth-child(2)').text
            self._curent_page_num = int(page_coursor_text.split(' ')[1])
        return self._curent_page_num

    @property
    def pages_count(self):
        if not hasattr(self, '_pages_count'):
            page_coursor_text = self.soup.select_one(
                '#srp-pagination > li:nth-child(1)').text
            if page_coursor_text == 'previous':
                page_coursor_text = self.soup.select_one('#srp-pagination > li:nth-child(2)').text
            self._pages_count = int(page_coursor_text.split(' ')[-1])
        return self._pages_count

    @property
    def total_article_count(self):
        if not hasattr(self, '_total_article_count'):
            self._total_article_count = self.pages_count * self.show_per_page
        return self._total_article_count

    def next_page(self):
        next_url = self.soup.select_one('li.next-link > a')
        try:
            href = next_url.get('href')
            return Search_page(urljoin('https://'+self.url_parts.netloc, href))
        except AttributeError:
            return None

    def export_bibtex(self, file):
        raise NotImplementedError
