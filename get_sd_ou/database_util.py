from hashlib import sha1
import json
from re import search
import mysql.connector
import logging

logger = logging.getLogger('mainLogger')

def init_db():
    logger.debug('[database_util][init_db][IN]')
    #cnx = mock_connection()
    cnx = mysql.connector.connect(
        host="localhost",
        user="sciencedirect",
        password="root",
        port='3306'
    )
    logger.debug('[database_util][init_db][OUT] | db_connection : %s', cnx)

    return cnx
class mock_cursor:
    def __init__(self, *args, **kwargs):
        print('cursor init', args, kwargs)
        self.lastrowid = -1
    def __getattribute__(self, *args, **kwargs):
        return lambda *args, **kwargs : print('cursor', '|', args, '|', kwargs)

class mock_connection:
    def __init__(self, *args, **kwargs):
        print('connection init', args, kwargs)
        self.cursor = mock_cursor()
    def __getattribute__(self, *args, **kwargs):
        return lambda *args, **kwargs : print('connection', '|', args, '|', kwargs)

#executeScriptsFromFile('/root/dev/sciencedirect-authors-data/db/scripts/sciencedirect.sql', database.cursor)
# cnx.commit()

def result_reper (res_dict):
    res_list = []
    filtered_items_list = list(filter(lambda x: x[1] not in [' ', '', [], None], res_dict.items()))
    for item in filtered_items_list:
        res_list.append(f'{item[0]}: {item[1]}')
    return " | ".join(res_list)

# INSERT


def insert_article(pii, title='', bibtex='', cnx=None, **kwargs):
    # update = UPDATE articles SET title=%S
    print(f'insert art {cnx}')
    cursor = cnx.cursor(buffered=True)
    sql = "INSERT IGNORE INTO sciencedirect.articles (pii, title, bibtex) VALUES (%s, %s, %s);"
    val = (pii, title, bibtex)
    logger.debug('[database_util][insert_article][IN] | pii: %s, sql: %s, val: %s', pii, sql, val)
    cursor.execute(sql, val)
    cnx.commit()
    article_id = cursor.lastrowid
    if not article_id:
        article_id = get_article(pii, cnx=cnx)['article_id']
    logger.debug(
        '[database_util][insert_article][OUT] | pii: %s  id: %s', pii, article_id)
    return article_id


def insert_author(first_name, last_name, email='', affiliation='', is_coresponde=False, id=None, cnx=None):
    print(f'auth {cnx}')
    name = last_name+'|'+first_name
    sql = "INSERT IGNORE INTO sciencedirect.authors (name, email, affiliation) \
            VALUES (%s, %s, %s)"
    
    val = (name, email, affiliation)
    logger.debug('[database_util][insert_author][IN] | name : %s , email: %s, aff: %s, scopus: %s',name, email, affiliation, id)
    cursor = cnx.cursor(buffered=True)
    cursor.execute(sql, val)
    cnx.commit()
    author_id = cursor.lastrowid
    if not author_id:
        print(author_id)
        author_id = get_author(first_name, last_name, email=email, cnx=cnx)['author_id']
    print('insert_auth DONE')
    return author_id

def get_article_author_id(article_id, author_id, cnx=None):
    print(f'ge art aut id {cnx}')
    cursor = cnx.cursor(buffered=True)
    sql = 'SELECT * FROM sciencedirect.article_authors WHERE article_id = %s AND author_id = %s'
    cursor.execute(sql, [article_id, author_id])
    fetch_res = cursor.fetchall()[-1]
    return 1 if fetch_res else 0

def get_search_article_id(search_id, article_id, cnx=None):
    print(f'g sea art id {cnx}')
    cursor = cnx.cursor(buffered=True)
    sql = 'SELECT * FROM sciencedirect.search_articles WHERE search_id = %s AND article_id = %s'
    cursor.execute(sql, [search_id, article_id])
    fetch_res = cursor.fetchall()[-1]
    return 1 if fetch_res else 0

def connect_article_author(article_id, author_id, is_corresponde=0, cnx=None):
    print(f'con art auth {cnx}')
    cursor = cnx.cursor(buffered=True)
    # TODO connect article with pii (get article id from articles from pii)
    sql = "INSERT IGNORE INTO sciencedirect.article_authors (article_id, author_id, is_corresponde) VALUES (%s, %s, %s);"
    val = (article_id, author_id, is_corresponde)
    cursor.execute(sql, val)
    cnx.commit()
    connection_id = cursor.lastrowid
    if not connection_id :
        connection_id = get_article_author_id(article_id, author_id, cnx=cnx)
    logger.debug(
        '[database_util][connect_article_author][OUT] | article_id: %s  author_id: %s, connection_id: %s', article_id, author_id, connection_id)
    return connection_id


def connect_search_article(search_id, article_id, cnx=None):
    print(f'conn sea art {cnx}')
    # TODO connect article with pii (get article id from articles from pii)
    sql = "INSERT IGNORE INTO sciencedirect.search_articles (search_id, article_id) VALUES (%s, %s);"
    val = (search_id, article_id)
    logger.debug('[database_util][connect_search_article][IN] | search_id : %s, article_id: %s', search_id, article_id)
    cursor = cnx.cursor(buffered=True)
    cursor.execute(sql, val)
    cnx.commit()
    connection_id = cursor.lastrowid
    if not connection_id :
        connection_id = get_search_article_id(search_id, article_id, cnx=cnx)
    logger.debug(
        '[database_util][connect_search_article][OUT] | search_id: %s  article_id: %s, connection_id: %s', search_id, article_id, connection_id)
    return connection_id

def insert_search(search_hash, cnx=None, **search_kwargs):
    print(f'ins sea{cnx}')
    logger.debug('[database_util][insert_search][IN] | search_hash : %s, search_kwargs : %s', search_hash, search_kwargs)
    cursor = cnx.cursor(buffered=True)
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
    cnx.commit()
    logger.debug('[database_util][insert_search][OUT] | sql : %s, val : %s, search_id : %s', sql, val, search_id)
    return search_id 

def insert_multi_author(authors_list, cnx=None):
    authors_id = []
    for author in authors_list:
        authors_id.append(insert_author(**author, cnx=cnx))
    return authors_id


def connect_multi_article_authors(article_id, authors_id_list, cnx=None):
    for author_id in authors_id_list:
        connect_article_author(article_id, author_id, cnx=cnx)


def insert_article_data(pii, authors, cnx=None, **kwargs):
    print('article_data', cnx)
    article_id = insert_article(pii=pii, cnx=cnx, **kwargs)

    authors_id = insert_multi_author(authors, cnx=cnx)
    connect_multi_article_authors(article_id, authors_id, cnx=cnx)
    return article_id

# UPDATE


def update_article(cnx=None):
    raise NotImplementedError


def update_author_scopus(name, id, cnx=None):
    print(f'upd auth scopus {cnx}')
    cursor = cnx.cursor(buffered=True)
    sql = 'UPDATE sciencedirect.authors SET scopus=%s WHERE name=%s LIMIT 1;'
    val = (id, name)
    cursor.execute(sql, val)
    cnx.commit()
    author_id = cursor.lastrowid
    return author_id


def update_search_offset(offset, hash, cnx=None):
    print(f'upd search off {cnx}')
    logger.debug('[database_util][update_search_offset][IN] | offset : %s, hash : %s', offset, hash)
    cursor = cnx.cursor(buffered=True)
    sql = 'UPDATE sciencedirect.searchs SET offset=%s WHERE hash=%s LIMIT 1;'
    val = (offset, hash)
    cursor.execute(sql, val)
    cnx.commit()
    search_id = cursor.lastrowid
    logger.debug('[database_util][update_search_offset][OUT] | search_id : %s', search_id)
    return search_id

# SELECT

def get_author(first_name, last_name, email='', cnx=None):
    print(f'g auth {cnx}')
    name = last_name+'|'+first_name
    logger.debug('[db_util][get_author][IN] | name: %s, email: %s', name, email)
    cursor = cnx.cursor(buffered=True, dictionary=True)
    sql = "SELECT * FROM sciencedirect.authors WHERE name = %s OR email = %s LIMIT 1"
    
    cursor.execute(sql, (name, email))
    fetch_res = cursor.fetchone()
    cursor.reset()
    print('get_au DONE')
    return fetch_res

def get_article(pii, cnx=None):
    print(f'art {cnx}')
    logger.debug('[database_util][get_article][IN] | pii : %s', pii)
    cursor = cnx.cursor(buffered=True, dictionary=True)
    sql = "SELECT * FROM sciencedirect.articles WHERE pii = %s LIMIT 1"
    
    cursor.execute(sql, (pii, ))
    fetch_res = cursor.fetchone()
    cursor.reset()
    logger.debug('[database_util][get_article][OUT] | fetch_res : %s', fetch_res)
    return fetch_res

def get_search_suggest(input_key, input_value, cnx=None):
    print(f'get sea sugg {cnx}')
    logger.debug('[database_util][get_search_suggest][IN] | input_key : %s, input_value : %s', input_key, input_value)
    cursor = cnx.cursor(buffered=True)
    sql = "SELECT " + str(input_key) + " FROM sciencedirect.searchs WHERE " + str(input_key) + " LIKE %s;"
    cursor.execute(sql, ('%'+input_value+'%', ))
    fetch_res = list(set([i[0] for i in cursor.fetchall()]))
    logger.debug('[database_util][get_search_suggest][OUT] | input_key : %s, input_value : %s, fetch_res : %s',  input_key, input_value, fetch_res)
    return fetch_res


def get_search_suggest_all(cnx=None, **search_kwargs):
    print(f'get search sugg all {cnx}')
    logger.debug('[database_util][get_search_suggest][IN] | search_kwargs : %s', search_kwargs)
    cursor = cnx.cursor(buffered=True, dictionary=True)
    sql = "SELECT * FROM sciencedirect.searchs WHERE "
    val = []
    for key, value in search_kwargs.items():
        if value:
            val.append('%'+value+"%")
            sql += key + ' LIKE %s AND '
    sql = sql[:-5]
    sql += ';'
    cursor.execute(sql, val)
    fetch_res = cursor.fetchall()
    logger.debug('[database_util][get_search_suggest][OUT] | search_kwargs : %s, fetch_res : %s', search_kwargs, fetch_res)
    return fetch_res


def get_search(search_hash, cnx=None):
    print(f'get sea {cnx}')
    logger.debug('[database_util][get_search][IN] | search_hash : %s', search_hash)
    cursor = cnx.cursor(buffered=True, dictionary=True)
    sql = "SELECT * FROM sciencedirect.searchs WHERE hash = %s LIMIT 1"
    
    cursor.execute(sql, (search_hash, ))
    fetch_res = cursor.fetchone()
    cursor.reset()
    logger.debug('[database_util][get_search][OUT] | fetch_res : %s', fetch_res)
    return fetch_res

def get_search_articles(search_id, cnx=None):
    print(f'get search art {cnx}')
    logger.debug('[database_util][get_search_articles][IN] | search_id : %s', search_id)
    cursor = cnx.cursor(buffered=True, dictionary=True)
    sql = "SELECT t3.*\
          FROM sciencedirect.searchs AS t1\
          JOIN sciencedirect.search_articles AS t2 ON t1.search_id = t2.search_id\
          JOIN sciencedirect.articles AS t3 ON t2.article_id = t3.article_id\
          WHERE t2.search_id = %s"

    val = (search_id, )
    cursor.execute(sql, val)

    myresult = cursor.fetchall()
    logger.debug('[database_util][get_search_articles][OUT] | result : %s', myresult)
    return myresult    

def get_db_result(cnx=None, **search_kwargs):
    logger.debug('[database_util][get_db_result][IN] | search_kwargs : %s', search_kwargs)
    searchs = {}
    for search in get_search_suggest_all(cnx=cnx, **search_kwargs):
        del search['hash']
        search_rep = result_reper(search)
        searchs[search_rep] = {}
        for article in get_search_articles(search['search_id'], cnx=cnx):
            article_rep = result_reper(article)
            searchs[search_rep][article_rep] = []
            for author in get_article_authors(article['article_id'], cnx=cnx):
                author_rep = result_reper(author)
                searchs[search_rep][article_rep].append(author_rep)
    logger.debug('[database_util][get_db_result][OUT] | searchs : %s', searchs)
    return searchs

def is_article_exist(pii, cnx=None):
    cursor = cnx.cursor(buffered=True)
    sql = "SELECT EXISTS (SELECT 1 FROM sciencedirect.article WHERE pii='%s' LIMIT 1)" # ADD s to article to fix I changed for some test :-)
    val = (pii, )
    cursor.execute(sql, val)
    _result = cursor.fetchone()
    cursor.reset() 
    
def is_row_exist(table, column, value, cnx=None):
    print(f'is row exc {cnx}')
    cursor = cnx.cursor(buffered=True)
    sql = "SELECT EXISTS(SELECT 1 FROM %s WHERE %s='%s' LIMIT 1)"
    val = (table, column, value)
    cursor.execute(sql, val)
    result = cursor.fetchone()
    cursor.reset() 
    return result


def get_id_less_authors(cnx=None):
    print(f'get id less {cnx}')
    logger.debug('[db_util][get_id_less_authors][IN]')
    cursor = cnx.cursor(buffered=True)
    sql = "SELECT name FROM sciencedirect.authors WHERE scopus is NULL"
    cursor.execute(sql)
    logger.debug('[db_util] [id_less_authors] one part got')
    chunk_size = 10
    names = cursor.fetchmany(chunk_size)
    while names:
        for name in names:
            yield name[0]
        names = cursor.fetchmany(chunk_size)
    logger.debug('[db_util][get_id_less_authors][OUT]')


def get_article_authors(article_id, cnx=None):
    print(f'get article auth {cnx}')
    logger.debug('[db_util][get_article_authors][IN] | article_id: %s', article_id)
    cursor = cnx.cursor(buffered=True, dictionary=True)
    sql = "SELECT t3.*\
          FROM sciencedirect.articles AS t1\
          JOIN sciencedirect.article_authors AS t2 ON t1.article_id = t2.article_id\
          JOIN sciencedirect.authors AS t3 ON t2.author_id = t3.author_id\
          WHERE t2.author_id = %s"

    val = (article_id, )
    cursor.execute(sql, val)

    myresult = cursor.fetchall()
    logger.debug('[db_util][get_article_authors][OUT] | result: %s', myresult)
    return myresult


def get_articles_of_author(sql, val, cnx=None):
    print(f'get art of auth {cnx}')
    cursor = cnx.cursor(buffered=True, cnx=cnx)
    cursor.execute(sql, val, cnx=cnx)


def executeScriptsFromFile(filename, cursor, cnx=None):
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
