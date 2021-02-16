import logging

from get_sd_ou.classUtil import Journal, JournalsSearch, Volume, Article
from queue import Queue
from threading import Thread, current_thread, Lock


from get_sd_ou.databaseUtil import insert_article_data, init_db


def scrape_and_save_article(article_url_queue, mysql_connection):
    first_url = article_url_queue.get()
    article_url_queue.put(first_url)

    while not article_url_queue.empty():
        url = article_url_queue.get()

        article_data, article_hash = scrape_article_url(url)
        if not article_data is None:
            save_article_to_db(article_data, mysql_connection)

            article_url_queue.task_done()
            add_to_persistance(article_hash, mysql_connection.cursor())
            logger.info(f"[{current_thread().name}] - Article scraped and saved - url = {url}")
        else:
            logger.info(f"[{current_thread().name}] skipped article: {url}")


def scrape_article_url(url):

    article = Article(url=url)
    article_hash = article.__hash__()
    if not article_hash in visited:
        article_data = article.get_article_data()

        logger.debug(f"thread: ({current_thread().name})[journal_scraper]-[scrape_article_url] | {url}")
        return article_data, article_hash
    return None, None

def save_article_to_db(article_data, db_connection):
    # todo pass journal_search, journal, volume to insert_article_data to add them all in one to database
    article_id = insert_article_data(**article_data, cnx=db_connection)
    logger.debug(f"thread: ({current_thread().name})[journal_scraper]-[save_article_to_db] | {article_id}")


def get_node_children(node):

    if node == "ROOT":
        yield from iterate_journal_searches()
    elif isinstance(node, JournalsSearch):
        yield from node.iterate_journals()
    elif isinstance(node, Journal):
        yield from node.iterate_volumes()
    elif isinstance(node, Volume):
        articles = node.get_articles()
        for article in articles:
            yield article
    else:
        raise Exception(f"Invalid node - ({type(node)}) - {node}")


def iterate_journal_searches():
    journal_search = JournalsSearch().get_next_page()
    while journal_search:
        yield journal_search
        journal_search = journal_search.get_next_page()


def deep_first_search_for_articles(self_node, article_url_queue, mysql_connection):
    if not self_node.__hash__() in visited:
        node_children = get_node_children(self_node)

        if isinstance(self_node, Volume):  # deepest node of tree before articles is Volume
            articles = list(node_children)
            list(map(article_url_queue.put, articles))
        else:
            for child in node_children:
                deep_first_search_for_articles(self_node=child, article_url_queue=article_url_queue, mysql_connection=mysql_connection)
        add_to_persistance(self_node.__hash__(), mysql_connection)
    else:
        logger.info(f"[{current_thread().name}] skipped node: {str(self_node)}")

def init_persistance():
    mysql_connection = init_db()
    mysql_cursor = mysql_connection.cursor()
    results = mysql_cursor.execute("create table if not exists sciencedirect.visited (hash INTEGER)")
    print("persistance made")

    return mysql_connection
        

def add_to_persistance(item, cnx):
    lock.acquire()
    visited.add(int(item))
    lock.release()
    cursor = cnx.cursor()
    res = cursor.execute(f'INSERT INTO sciencedirect.visited VALUES ({int(item)})')


def write_visited(write_set, mysql_connection=None):
    res = None
    cursor = mysql_connection.cursor()
    for i in write_set:
        res = cursor.execute(f'INSERT INTO sciencedirect.visited VALUES ({int(i)})')
    mysql_connection.commit()
    print(res)


def load_visited(mysql_connection=None):
    cursor = mysql_connection.cursor()
    res = cursor.execute('SELECT hash FROM sciencedirect.visited')
    if res is None:
        return set()
    else:
        return set([i[0] for i in res])

if __name__ == "__main__":

    logger = logging.getLogger('mainLogger')
    logger.setLevel(logging.INFO)

    mysql_connection = init_persistance()

    file_data = load_visited(mysql_connection)
    

    visited = file_data if file_data else set()

    lock = Lock()

    article_queue = Queue(maxsize=500)
    search_thread = Thread(target=deep_first_search_for_articles,
                           args=("ROOT", article_queue, mysql_connection))
    try:
        search_thread.start()

        for i in range(15):
            mysql_connection = init_persistance()

            t = Thread(target=scrape_and_save_article, args=(article_queue, mysql_connection))
            t.start()

        article_queue.join()
    except Exception as e:
        print(e)
        print("EXCEPTION")

