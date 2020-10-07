import queue
import threading
import logging

from .class_util import Article, Search_page
from .database_util import insert_article_data

logger = logging.getLogger('mainLogger')

def next_year_gen(init_year=2020, year_step=-1):
    """ iterate through all year fron init_year """
    current_search_year = init_year
    while(True):
        yield current_search_year
        current_search_year += year_step

def next_page_gen(year, show_per_page=100):
    """ iterate through page of year """
    page = 1
    logger.debug('[ main ] [ next_page_gen ] __init__ | year: %s, page=%s', year, page)
    search_url = f'https://www.sciencedirect.com/search?date={year}&show={show_per_page}&sortBy=date'
    while True :
        logger.debug('[ main ] [ next_page_gen ] next page | year: %s, page=%s, url: %s', year, page, search_url)
        yield {'url':search_url, 'page_number':page, 'year':year}
        page += 1
        search = Search_page(search_url)
        search_url = search.next_page()

def worker():
    global next_page_gen_obj
    continue_search = True
    while continue_search:
        if main_queue.empty():
            next_page = next(next_page_gen_obj)
            if next_page['url']:
                logger.debug('[ worker ] get artciles from year: %s , page: %s', next_page['year'], next_page['page_number'])

                search = Search_page(next_page['url'])
                articles = search.get_articles()
                [main_queue.put(article) for article in articles]

                logger.debug('[ worker ] page artciles got | year: %s , page: %s', next_page['year'], next_page['page_number'])
            else:
                next_year = next(next_year_gen_obj)
                logger.debug('[ worker ] go to next year search: %s', next_year)
                next_page_gen_obj = next_page_gen(next_year)

                continue
        article_url = main_queue.get()
        article = Article(article_url, headers)
        logger.debug('[ worker ] get data of article | pii : %s', article.pii)
        article_data = article.get_article_data()
        article_data['authors'] = [author.name for author in article_data['authors']]
        insert_article_data(*article_data.values())
        
        main_queue.task_done()

headers = {
        'Accept' : 'application/json',
        'user-agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/80.0'
        }

base_url = 'https://www.sciencedirect.com/'
article_test_url = 'https://www.sciencedirect.com/science/article/pii/S0950423020305763#!'
article_history = {} 
author_history = {}
start_year = 2010
end_year = 2020

year = '2020'
show_per_page = 25

main_queue = None
threads = None
next_year_gen_obj = None
next_page_gen_obj = None

def start_search(init_year):
    global threads
    global main_queue
    global next_year_gen_obj
    global next_page_gen_obj
    """ 
    1) Initiate the threads
    2) Initiate the database connection
    3) Call the worker function of each thread
    """
    next_year_gen_obj = next_year_gen(init_year=init_year)
    next_page_gen_obj = next_page_gen(next(next_year_gen_obj), show_per_page=show_per_page)
    main_queue = queue.Queue()
    #threads = [threading.Thread(target=worker) for _ in range(2)]
    #[thread.start() for thread in threads]
    #logger.debug('[ main ] threads started')
    worker()
    main_queue.join()
    return {'status':'200', 'msg':'threads started succesfully', 'threads':threads}

def pause_search():
    logger.info('[ main ] search paused')
    pass
def stop_search():
    """ 
    1) Commit current state to database
    2) kill threads
    """
    logger.info('[ main ] search stoped')
    pass
if __name__ == "__main__":
    logger.debug('___________________________________________[ Search Start ]________________________________________')
    start_search(2018)
