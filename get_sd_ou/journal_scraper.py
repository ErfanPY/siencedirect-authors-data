import pickle
import signal
import sys
import logging
import os

from get_sd_ou.classUtil import Journal, JournalsSearch, Volume, Article
from queue import Queue
from threading import Thread, current_thread
import sqlite3


# TODO: Do it asynchronously
from get_sd_ou.databaseUtil import insert_article_data, init_db


def scrape_and_save_article(article_url_queue, db_connection):
    first_url = article_url_queue.get()
    article_url_queue.put(first_url)

    while not article_url_queue.empty():
        url = article_url_queue.get()

        article_data = scrape_article_url(url)
        save_article_to_db(article_data, db_connection)

        article_url_queue.task_done()
        logger.info(f"[{current_thread().name}] - Article scraped and saved - url = {url}")


def scrape_article_url(url):

    article = Article(url=url)
    article_data = article.get_article_data()

    logger.debug(f"thread: ({current_thread().name})[journal_scraper]-[scrape_article_url] | {url}")
    return article_data


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
        write_visited(visited, db_cursor)
        for article in articles:
            yield article
    else:
        raise Exception(f"Invalid node - ({type(node)}) - {node}")


def iterate_journal_searches():
    journal_search = JournalsSearch().get_next_page()
    while journal_search:
        yield journal_search
        journal_search = journal_search.get_next_page()


def deep_first_search_for_articles(self_node, article_url_queue):
    if self_node.__hash__() in visited:
        return
    node_children = get_node_children(self_node)

    if isinstance(self_node, Volume):  # deepest node of tree before articles is Volume
        articles = list(node_children)
        list(map(article_url_queue.put, articles))
        node_children = []

    for child in node_children:
        graph[self_node] = graph.get(self_node, list()) + [child]
        deep_first_search_for_articles(self_node=child, article_url_queue=article_url_queue)
    visited.add(self_node.__hash__())

def init_persistance(cursor=None):
    results = cursor.execute("create table if not exists visited (hash INTEGER)")
    print("persistance made")
        

def write_visited(write_set, cursor=None):
    res = None
    for i in write_set:
        res = cursor.execute(f'INSERT INTO visited VALUES ({int(i)})')
    conn.commit()
    print(res)

    # with open("visited.txt", "w") as file:
    #     for i in write_set:
    #         file.write(str(i)+"\n")


def load_visited(cursor=None):
    results = cursor.execute('SELECT * FROM visited')
    return set(results)

        
    # if not os.path.exists("visited.txt"):
    #     return set()
    # with open("visited.txt", "r") as file:
    #     return set([i.strip() for i in file.readlines()])


if __name__ == "__main__":

    logger = logging.getLogger('mainLogger')
    logger.setLevel(logging.INFO)

    conn = sqlite3.connect('example.db', check_same_thread=False)

    db_cursor = conn.cursor()
    init_persistance(db_cursor)

    file_data = load_visited(db_cursor)
    visited = file_data if file_data else set()
    graph = {}

    article_queue = Queue(maxsize=500)
    search_thread = Thread(target=deep_first_search_for_articles,
                           args=("ROOT", article_queue))
    try:
        search_thread.start()

        for i in range(5):
            database_connection = init_db()

            t = Thread(target=scrape_and_save_article, args=(article_queue, database_connection))
            t.start()

        article_queue.join()
    except Exception as e:
        print(e)
        print("EXCEPTION")
    finally:
        write_visited(visited, db_cursor)
        # conn.close()
