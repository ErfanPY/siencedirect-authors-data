from hashlib import sha1
import json
from re import search
import mysql.connector
import logging

logger = logging.getLogger('mainLogger')


cnx = mysql.connector.connect(
    host="localhost",
    user="sciencedirect",
    password="root",
    port='3306'
)
cursor = cnx.cursor(buffered=True)

#executeScriptsFromFile('/root/dev/sciencedirect-authors-data/db/scripts/sciencedirect.sql', database.cursor)
# cnx.commit()

# INSERT


def insert_article(pii, title='', bibtex='', **kwargs):
    # update = UPDATE articles SET title=%S
    sql = "INSERT IGNORE INTO sciencedirect.articles (pii, title, bibtex) VALUES (%s, %s, %s);"
    val = (pii, title, bibtex)
    cursor.execute(sql, val)
    cnx.commit()
    article_id = cursor.lastrowid
    logger.debug(
        '[ database ] article inserted | pii: %s  id: %s', pii, article_id)
    return article_id


def insert_author(first_name, last_name, email='', affiliation='', is_coresponde=False, id=None):
    name = last_name+'|'+first_name
    sql = "INSERT IGNORE INTO sciencedirect.authors (name, email, affiliation, scopus) \
            VALUES (%s, %s, %s, %s)"

    val = (name, email, affiliation, id)
    cursor.execute(sql, val)
    cnx.commit()
    author_id = cursor.lastrowid
    logger.debug(
        '[ database ] author inserted | name: %s  id: %s', name, author_id)
    return author_id


def connect_article_author(article_id, author_id, is_corresponde=0):
    # TODO connect article with pii (get article id from articles from pii)
    sql = "INSERT IGNORE INTO sciencedirect.article_authors (article_id, author_id, is_corresponde) VALUES (%s, %s, %s);"
    val = (article_id, author_id, is_corresponde)
    cursor.execute(sql, val)
    cnx.commit()
    logger.debug(
        '[ database ] article and author connected | article_id: %s  author_id: %s', article_id, author_id)


def connect_search_article(search_id, article_id):
    # TODO connect article with pii (get article id from articles from pii)
    sql = "INSERT IGNORE INTO sciencedirect.search_articles (search_id, article_id) VALUES (%s, %s);"
    val = (search_id, article_id)
    cursor.execute(sql, val)
    cnx.commit()
    logger.debug(
        '[ database ] search and article connected | search_id: %s  article_id: %s', search_id, article_id)

def insert_search(search_hash, **search_kwargs):
    logger.debug('[database_util][insert_search][IN] | search_hash : %s, search_kwargs : %s', search_hash, search_kwargs)
    sql = "INSERT INTO sciencedirect.searchs (hash, date, qs, pub, authors, affiliation, volume, issue, page, tak, title, refrences, docId) VALUES ("
    val = []
    key_list = ['date', 'qs', 'pub', 'authors', 'affiliation', 'volume', 'issue', 'page', 'tak', 'title', 'refrences', 'docId']
    
    val.append(search_hash)
    sql += '%s, '
    
    for _ in key_list:
        sql += '%s, '
    sql = sql[:-2]
    sql += ');'

    for key in key_list:
        value = search_kwargs.get(key, '')
        value = value if value != None else ''
        val.append(value)
    cursor.execute(sql, val)
    search_id = cursor.lastrowid
    logger.debug('[database_util][insert_search][OUT] | sql : %s, val : %s', sql, val)
    return search_id 

def insert_multi_author(authors_list):
    authors_id = []
    for author in authors_list:
        authors_id.append(insert_author(**author))
    return authors_id


def connect_multi_article_authors(article_id, authors_id_list):
    for author_id in authors_id_list:
        connect_article_author(article_id, author_id)


def insert_article_data(pii, authors, **kwargs):
    article_id = insert_article(pii=pii)

    authors_id = insert_multi_author(authors)
    connect_multi_article_authors(article_id, authors_id)
    return article_id

# UPDATE


def update_article():
    raise NotImplementedError


def update_author_scopus(name, id):
    sql = 'UPDATE sciencedirect.authors SET scopus=%s WHERE name=%s LIMIT 1;'
    val = (id, name)
    cursor.execute(sql, val)
    cnx.commit()
    author_id = cursor.lastrowid
    return author_id


def update_search_offset(offset, hash):
    sql = 'UPDATE sciencedirect.searchs SET offset=%s WHERE hash=%s LIMIT 1;'
    val = (offset, hash)
    cursor.execute(sql, val)
    cnx.commit()
    search_id = cursor.lastrowid
    return search_id

# SELECT

def get_search_suggest(**search_kwargs):
    logger.debug('[database_util][get_search_suggest][IN] | search_kwargs : %s', search_kwargs)
    sql = "SELECT * FROM sciencedirect.searchs WHERE "
    val = []
    for key, value in search_kwargs.items():
        if value:
            val.append(key)
            val.append(value)
            sql += '%s LIKE %s AND '
    sql = sql[:-5]
    sql += ';'
    cursor.execute(sql, val)
    fetch_res = cursor.fetchall()
    logger.debug('[database_util][get_search_suggest][OUT] | search_kwargs : %s, fetch_res : %s', search_kwargs, fetch_res)
    return fetch_res


def get_search(search_hash):
    logger.debug('[database_util][get_search][IN] | search_hash : %s', search_hash)
    sql = "SELECT * FROM sciencedirect.searchs WHERE hash = %s"
    
    cursor.execute(sql, (search_hash, ))
    fetch_res = cursor.fetchone()
    logger.debug('[database_util][get_search][OUT] | fetch_res : %s', fetch_res)
    return fetch_res


def is_row_exist(table, column, value):
    sql = "SELECT EXISTS(SELECT 1 FROM %s WHERE %s='%s' LIMIT 1)"
    val = (table, column, value)
    cursor.execute(sql, val)
    result = cursor.fetchone()
    return result


def get_id_less_authors():
    logger.debug('[db_util] getting id less authors')
    sql = "SELECT name FROM sciencedirect.authors WHERE scopus is NULL"
    cursor.execute(sql)
    logger.debug('[db_util] [id_less_authors] one part got')
    chunk_size = 10
    names = cursor.fetchmany(chunk_size)
    while names:
        for name in names:
            yield name[0]
        names = cursor.fetchmany(chunk_size)
    logger.debug('[db_util] id_less_authors name got from database')


def get_article_authors(article_id):
    sql = "SELECT t1.title, t3.name\
          FROM articles AS t1\
          JOIN article_authors AS t2 ON t1.article_id = t2.article_id\
          JOIN authors AS t3 ON t2.author_id = t3.author_id\
          WHERE t2.author_id = %s"

    val = (article_id, )
    cursor.execute(sql, val)

    myresult = cursor.fetchall()
    return myresult


def get_articles_of_author(sql, val):
    cursor.execute(sql, val)


def executeScriptsFromFile(filename, cursor):
    # Open and read the file as a single buffer
    fd = open(filename, 'r')
    sqlFile = fd.read()
    fd.close()

    # all SQL commands (split on ';')
    sqlCommands = sqlFile.split(';')
    # Execute every command from the input file
    for command in sqlCommands:
        # This will skip and report errors
        # For example, if the tables do not yet exist, this will skip over
        # the DROP TABLE commands
        try:
            if command.rstrip() != '':
                cursor.execute(command)
        except ValueError as msg:
            print("Command skipped: ", msg)
