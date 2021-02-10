from hashlib import sha1
import json
import logging
import re
from urllib.parse import parse_qsl, unquote_plus, urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup as bs

logger = logging.getLogger('mainLogger')

retry_strategy = Retry(
    total=3,
    status_forcelist=[429, 500, 502, 503, 504],
    method_whitelist=["HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.mount("http://", adapter)


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

    def join_url_path_to_self_netloc(self, url_path):
        return urljoin('https://' + self.url_parts.netloc, url_path)

    @property
    def response(self):
        try:
            self.__getattribute__('_response')
            logger.debug('[ Url ] response exist')
            return self._response
        except AttributeError:
            logger.debug('[ Url ] getting url response | url: %s', self.url)
            self._response = http.get(self.url, headers=self.headers)
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


class Page(Url):
    def __init__(self, url, do_soup=False, soup_data=None, **kwargs):
        logger.debug('[ Page ] __init__ | url: %s', url)
        super().__init__(url=url, soup=None)

        self.seen_count = 0
        self.text = ''
        if soup_data:
            self._soup = soup_data
        elif do_soup:
            self._soup = self._soup_maker()

    def __hash__(self):
        return hash(self.url.url_parts[1:3])

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
            raise (requests.exceptions.ConnectionError(
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
        return (self[key])


def filter_list_in_dict(dict_data, check_key, expected_value, just_first=False):
    matched_list = list(filter(lambda x: x[check_key] == expected_value, dict_data))
    if matched_list:
        return (matched_list[0] if just_first else matched_list)
    return {} if just_first else [{}, ]


class Article(Page):
    def __init__(self, url, do_bibtex=False, soup_data=None, *args, **kwargs):
        self.url = url
        self.pii = self.get_pii()

        super().__init__(url, soup_data=soup_data, *args, **kwargs)

        self.bibtex = ''
        self.bibtex_url = None

        self._title = ''
        self._keywords = ''

        if do_bibtex:
            self.bibtex = self.export_bibtex()

        self._authors = None

    def get_pii(self):
        return self.url.split('/')[-1].replace('#!', '')

    @property
    def keywords(self):
        if self._keywords:
            return self._keywords

        keywords = ''
        keywords_container = self.soup.select_one('.Keywords')
        if not keywords_container:
            return ''
        for keyword_group in keywords_container.select('.keywords-section'):
            for keyword in keyword_group.select('.keyword'):
                keywords += keyword.text + '|'

        self._keywords = keywords
        return self._keywords

    def get_article_data(self):
        """ this is the main function of article it collect all data we need from an article (needed data is specified from input)
        it get authors name and email and affiliation from article 
        """
        data = {'pii': self.pii, 'authors': self.authors, 'bibtex': self.bibtex, 'title': self.title,
                'keywords': self.keywords}

        return data

    def export_bibtex(self):
        self.bibtex_url = Url(
            f'https://www.sciencedirect.com/sdfe/arp/cite?pii={self.pii}&format=text/x-bibtex&wi')
        self.bibtex_file_path = f'articles/{self.pii}.bib'
        with open(self.bibtex_file_path, 'ab') as f:
            f.write(http.get(self.bibtex_url, headers=self.headers))
        return self.bibtex_url

    @staticmethod
    def _author_icons(tag_a):
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
        # affiliations_list = re.findall(r'country[^\]\}]*"([^\]\}]*)"', json_element)
        # if not affiliations_list :
        #     #TODO
        #     affiliations_list = [' ']
        authors_groups_list_json = json_data['authors']['content']
        authors_groups_list_json = list(
            filter(lambda dict: dict['#name'] == 'author-group', authors_groups_list_json))

        for group in authors_groups_list_json:  # in authors maybe some group which devides authors
            group_authors = list(
                filter(lambda dict: dict['#name'] == 'author', group['$$']))
            [authors_list_json.append(group_author) for group_author in group_authors]

        affiliations_data_dict = json_data['authors']['affiliations']
        for index, author_json in enumerate(authors_list_json):
            reference_list = list(filter(lambda dict: dict['#name'] == 'cross-ref', author_json['$$']))
            affiliations_id_list = [ref['$']['refid'] for ref in reference_list if 'aff' in ref['$']['refid']]
            affiliation_text = ''
            for affiliation_id in affiliations_id_list:
                affiliation_json = affiliations_data_dict[affiliation_id]
                affiliation_fn = list(filter(lambda dict: dict['#name'] == 'textfn', affiliation_json['$$']))[0]
                if affiliation_fn.get('$$'):
                    affiliation_text_list = list(filter(lambda dict: dict['#name'] == '__text__', affiliation_fn['$$']))
                    for affiliation_text_item in affiliation_text_list:
                        affiliation_text += affiliation_text_item['_'] + '||'
                else:
                    affiliation_text += affiliation_fn['_'] + '||'

            first_name = filter_list_in_dict(author_json['$$'], '#name', 'given-name', just_first=True).get('_',
                                                                                                            'noFirstName')
            last_name = filter_list_in_dict(author_json['$$'], '#name', 'surname', just_first=True).get('_',
                                                                                                        'noLastName')
            email_check = filter_list_in_dict(author_json['$$'], '#name', 'e-address', just_first=True)
            try:
                email = email_check['_'] if email_check else None
            except KeyError:
                email = email_check['$$'][0]['_']

            authors_res[index] = {'first_name': first_name, 'last_name': last_name,
                                  'email': email, 'affiliation': affiliation_text}
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
                logger.debug('Author got, %s', authors_data[index])

            authors_objects = [Author(**author_data)
                               for author_data in authors_data.values()]
            self._authors = authors_objects
            logger.debug('[ Article ] authors: %s', self._authors)
        return self._authors

    @property
    def title(self):
        if not self._title:
            self._title = self.soup.select_one('.title-text').text
        return self._title


class SearchPage(Page):
    def __init__(self, url='', show_per_page=100, start_offset=0, soup_data=None, **search_kwargs):
        if not url:
            # url = f'https://www.sciencedirect.com/search?qs={title}&date={url}&authors={author}&affiliations={affiliation}&show={show_per_page}'
            url = 'https://www.sciencedirect.com/search?'
            for key, value in search_kwargs.items():
                if value:
                    url += '{}={}&'.format(key, value)
            url += 'show={}&'.format(show_per_page)

        logger.debug('[ SearchPage ] __init__ | url: %s', url)
        super().__init__(url, soup_data=soup_data)

        self.url = url
        self.query = dict(self.url_parts.query)
        self.search_kwargs = self.query
        self.show_per_page = show_per_page
        self.offset = self.query.get('offset', start_offset)
        self.offset = self.offset if self.offset != '' else 0
        self.search_kwargs['offset'] = self.offset

    def db_hash(self):
        return sha1(json.dumps(self.search_kwargs, sort_keys=True, ensure_ascii=False).encode('utf-8')).hexdigest()

    def __bool__(self):
        return self.url != ''

    def get_articles(self):
        logger.debug('[ SearchPage ] getting articles | url: %s', self.url)
        search_result = self.soup.find_all('a')
        articles = []
        for article in search_result:
            if article.get('href'):
                article_link = article.get('href')
                if 'pii' in article_link and not 'pdf' in article_link and not (
                        article_link.split('/')[-1].startswith('B')):
                    articles.append(
                        urljoin('https://' + self.url_parts.netloc, article_link))
                    logger.debug(
                        '[ SearchPage ] one article added | url: %s', self.url)
        logger.debug('[ SearchPage ] all articels got | url: %s', self.url)
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
            return SearchPage(urljoin('https://' + self.url_parts.netloc, href))
        except AttributeError:
            return None


class Volume(SearchPage):
    def __init__(self, url, **kwargs):
        super().__init__(url=url, **kwargs)

    def get_previous(self):
        previous_volume = self.soup.select_one('.u-padding-xs-hor > div:nth-child(1) > a:nth-child(1)')

        return previous_volume.get('href', False)

    def get_next(self):
        pass


class Journal(Page):
    def __init__(self, url='', journal_name='', page_kwargs={}, **kwargs):
        # https://www.sciencedirect.com/journal/aace-clinical-case-reports/articles-in-press
        logger.debug('[ SearchPage ] __init__ | url: %s', url)
        super().__init__(url, **kwargs)
        if not url:
            # url = 'https://www.sciencedirect.com/journal/aace-clinical-case-reports/articles-in-press?page=2'
            url = 'https://www.sciencedirect.com/journal/{journal_name}/articles-in-press?'

            for key, value in page_kwargs.items():
                if value:
                    url += '{}={}&'.format(key, value)

        self.url = url
        self.page_kwargs = page_kwargs
        self.kwargs = kwargs
        self.query = dict(self.url_parts.query)
        self.search_kwargs = self.query
        self.page = self.query.get('page')
        self.page = self.page if self.page != '' else 1
        self.search_kwargs['page'] = self.page
        self.journal_name = journal_name if journal_name else self.soup.select_one('.anchor-text').text

    def db_hash(self):
        return sha1(json.dumps(self.search_kwargs, sort_keys=True, ensure_ascii=False).encode('utf-8')).hexdigest()

    def __bool__(self):
        return self.url != ''

    def iterate_volumes(self):
        last_issue_url = self.get_last_issue_url()
        last_issue = Volume(url=last_issue_url)

        yield last_issue
        previous_issue_url = ''
        previous_issue_path = last_issue.get_previous()

        while previous_issue_path:
            previous_issue_url = self.join_url_path_to_self_netloc(previous_issue_path)
            previous_issue = Volume(url=previous_issue_url)

            yield previous_issue
            previous_issue_path = previous_issue.get_previous()

        # issues_page = Page(url=self.url+"/issues")
        # search_result = issues_page.soup.find_all('a')
        # journals = set()
        #
        # for journal in search_result:
        #     journal_link = journal.get('href')
        #
        #     link_path = Url(journal_link).url_parts.path
        #
        #     if "vol" in link_path.split('/'):
        #         journals.add(
        #             urljoin('https://' + self.url_parts.netloc, journal_link))
        #
        # return journals


    def get_last_issue_url(self):
        issues_iterator = self.soup.select_one("div.issue").children
        last_issue_path = list(issues_iterator)[0].get('href')
        last_issue_url = self.join_url_path_to_self_netloc(last_issue_path)
        return last_issue_url

    @property
    def curent_page_num(self):
        if not hasattr(self, '_curent_page_num'):
            page_coursor_text = self.soup.select_one('pagination-pages-label').text
            self._curent_page_num = int(page_coursor_text.split('of')[0].split('page')[-1])
        return self._curent_page_num

    @property
    def pages_count(self):
        if not hasattr(self, '_pages_count'):
            page_coursor_text = self.soup.select_one('pagination-pages-label').text
            self._pages_count = int(page_coursor_text.split('of')[-1])
        return self._pages_count

    def next_page(self):
        if self.curent_page_num < self.pages_count:
            next_journal = Journal(journal_name=self.journal_name, page_kwargs={'page': self.curent_page_num + 1},
                                   **self.kwargs)
            return next_journal.url


class JournalsSearch(Page):
    def __init__(self, url='', letter='', start_page=1, soup_data=None, **search_kwargs):
        if not url:
            # url = 'https://www.sciencedirect.com/browse/journals-and-books?contentType=JL&searchPhrase=nano'
            url = f'https://www.sciencedirect.com/browse/journals-and-books/{letter}?'
            if start_page != 1: search_kwargs['page'] = start_page
            for key, value in search_kwargs.items():
                if value:
                    url += '{}={}&'.format(key, value)

        logger.debug('[ SearchPage ] __init__ | url: %s', url)
        super().__init__(url, soup_data=soup_data)

        self.url = url
        self.letter = letter
        self.query = dict(self.url_parts.query)
        self.search_kwargs = self.query
        self.page_num = self.query.get('page', start_page)
        self.page_num = int(self.page_num) if self.page_num != '' else 1
        self.search_kwargs['page'] = self.page_num
        self._pages_count = None

    def db_hash(self):
        return sha1(json.dumps(self.search_kwargs, sort_keys=True, ensure_ascii=False).encode('utf-8')).hexdigest()

    def __bool__(self):
        return self.url != ''

    def iterate_journals(self):
        # TODO: get_journal should return all journal of a letter

        journals = self._get_page_journals(self)
        for journal in journals:
            yield Journal(journal)
        next_journal_search_page = self.get_next_page()

        while next_journal_search_page:
            next_journals = next_journal_search_page._get_page_journals(next_journal_search_page)
            for next_journal in next_journals:
                yield Journal(next_journal)
            next_journal_search_page = next_journal_search_page.get_next_page()

    @staticmethod
    def _get_page_journals(journal_search_page):
        journals_list_div = journal_search_page.soup.select_one("#publication-list")
        search_result = journals_list_div.find_all('a')
        journals = []

        for journal in search_result:
            journal_path = journal.get('href')
            if any([check in journal_path for check in ['handbook', 'journal', 'bookseries']]):
                journals.append(urljoin('https://' + journal_search_page.url_parts.netloc, journal_path))

        return journals

    def get_next_page(self):
        if self.page_num < self.pages_count:
            return JournalsSearch(letter=self.letter, start_page=self.page_num + 1, **self.search_kwargs)
        return False

    @property
    def pages_count(self):
        if self._pages_count is None:
            page_counter_text = self.soup.select_one('.pagination-pages-label').text
            self._pages_count = int(page_counter_text.split('of')[-1])
        return self._pages_count


# class root(Page):
#     def __init__(self, url='', search_kwargs: dict[str, str] = {}, **kwargs):
#         if not url:
#             # url = 'https://www.sciencedirect.com/browse/journals-and-books?contentType=JL&searchPhrase=nano'
#             url = 'https://www.sciencedirect.com/browse/journals-and-books?'
#
#             for key, value in search_kwargs.items():
#                 if value:
#                     url += '{}={}&'.format(key, value)
#
#         logger.debug('[ SearchPage ] __init__ | url: %s', url)
#         super().__init__(url, **kwargs)
#
#         self.url = url
#         self.query = dict(self.url_parts.query)
#         self.search_kwargs = self.query
#         self.page = self.query.get('page', 1)
#         self.page = self.page if self.page != '' else 1
#         self.search_kwargs['page'] = self.page
#
#     def get_child(self, list_selector, child_selector, includes, excludes):
#         """ child_type : div, a,  """
#         childs_list_div = self.soup.select_one(list_selector)
#         search_result = childs_list_div.select(child_selector)
#         childs = []
#         for child in search_result:
#             child_link = child.get('href')
#             include_check = all([i in child_link for i in includes])
#             exclude_check = all([i not in child_link for i in excludes])
#             if child_link and include_check and exclude_check:
#                 childs.append(
#                     urljoin('https://' + self.url_parts.netloc, child_link))
#         logger.debug('[ SearchPage ] all childs got | url: %s', self.url)
#         return childs
#
#     def get_next(self):
#         pass
#
#     def attributes(self):
#         """  """
#         pass
