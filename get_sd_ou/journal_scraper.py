import queue
from get_sd_ou.classUtil import Journal, JournalsSearch, Volume
import requests
from queue import Queue
from threading import Thread

# TODO: Do it asynchronously


def scraper_worker(q):
    while not q.empty():
        url = q.get()
        # r = requests.get(url)
        # page = pyquery(r.text)
        # data = page("#data").text()
        q.task_done()


def get_node_children(node):
    if node == "ROOT":
        journal_search = JournalsSearch()
        yield journal_search
        for next_journal_search in journal_search.next_page():
            yield next_journal_search

    elif isinstance(node, JournalsSearch):
        return node.get_journals()
    elif isinstance(node, Journal):
        return Journal.get_volumes()
    elif isinstance(node, Volume):
        return Journal.get_articles()
    else:
        raise Exception("Invalid node")    


def deep_first_search_for_articles(node, article_queue, grapgh=[], visited: set = ()):  # TODO: graph adn visited
    node_children = get_node_children(node)
    
    if isinstance(node, Volume): # deepest node of tree before articles is Volume
        map(article_queue.put, list(node_children))
    
    for child in node_children:
        deep_first_search_for_articles(child, article_queue=article_queue)


if __name__ == "__main__":
    article_queue = Queue()
    search_thread = Thread(target=deep_first_search_for_articles, kwargs={"node": "ROOT", "article_queue": article_queue})
    search_thread.start()
    
    for i in range(5):
        t = Thread(target=scraper_worker, args=(article_queue, ))
        t.start()
    
    article_queue.join()
