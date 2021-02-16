import logging

import mysql.connector

logger = logging.getLogger('mainLogger')


def init_db(host="localhost", user="sciencedirect", password="root", port='3306'):
    logger.debug('[databaseUtil][init_db][IN]')
    cnx = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        port=port
    )
    logger.debug('[databaseUtil][init_db][OUT] | db_connection : %s', cnx)

    return cnx

# INSERT


def insert_article(pii, title='', bibtex='', keywords='', cnx=None, **kwargs):
    cursor = cnx.cursor(buffered=True)
    sql = 'INSERT IGNORE INTO sciencedirect.articles (pii, title, bibtex, keywords) VALUES (%s, %s, %s, %s);'
    val = (pii, title, bibtex, keywords)
    logger.debug('[databaseUtil][insert_article][IN] | pii: %s, sql: %s, val: %s', pii, sql, val)
    cursor.execute(sql, val)
    cnx.commit()
    article_id = cursor.lastrowid
    if not article_id:
        article_id = get_article(pii, cnx=cnx)['article_id']
    logger.debug(
        '[databaseUtil][insert_article][OUT] | pii: %s  id: %s', pii, article_id)
    return article_id


def insert_author(first_name, last_name, email='', affiliation='', is_coresponde=False, id=None, cnx=None):
    name = last_name + '|' + first_name
    sql = "INSERT IGNORE INTO sciencedirect.authors (name, email, affiliation) \
            VALUES (%s, %s, %s)"

    val = (name, email, affiliation)
    logger.debug('[databaseUtil][insert_author][IN] | name : %s , email: %s, aff: %s, scopus: %s', name, email,
                 affiliation, id)
    cursor = cnx.cursor(buffered=True)
    cursor.execute(sql, val)
    cnx.commit()
    id = cursor.lastrowid
    if not id:
        id = get_author(first_name, last_name, email=email, cnx=cnx)['author_id']
    return id


def get_article_author_id(article_id, author_id, cnx=None):
    cursor = cnx.cursor(buffered=True)
    sql = 'SELECT * FROM sciencedirect.article_authors WHERE article_id = %s AND author_id = %s'
    cursor.execute(sql, [article_id, author_id])
    fetch_res = cursor.fetchall()[-1]
    return 1 if fetch_res else 0


def connect_article_author(article_id, author_id, is_corresponde=0, cnx=None):
    cursor = cnx.cursor(buffered=True)
    sql = "INSERT IGNORE INTO sciencedirect.article_authors " \
          "(article_id, author_id, is_corresponde) VALUES (%s, %s, %s);"
    val = (article_id, author_id, is_corresponde)
    cursor.execute(sql, val)
    cnx.commit()
    connection_id = cursor.lastrowid
    if not connection_id:
        connection_id = get_article_author_id(article_id, author_id, cnx=cnx)
    logger.debug(
        '[databaseUtil][connect_article_author][OUT] | article_id: %s  author_id: %s, connection_id: %s', article_id,
        author_id, connection_id)
    return connection_id


def insert_multi_author(authors_list, cnx=None):
    authors_id = []
    for author in authors_list:
        authors_id.append(insert_author(**author, cnx=cnx))
    return authors_id


def connect_multi_article_authors(article_id, authors_id_list, cnx=None):
    for author_id in authors_id_list:
        connect_article_author(article_id, author_id, cnx=cnx)


def insert_article_data(pii, authors, cnx=None, **kwargs):
    article_data = get_article(pii, cnx)

    if not article_data:
        article_id = insert_article(pii=pii, cnx=cnx, **kwargs)
    else:
        article_id = article_data.get('article_id')

    authors_id = insert_multi_author(authors, cnx=cnx)
    connect_multi_article_authors(article_id, authors_id, cnx=cnx)
    return article_id

# SELECT

def get_author(first_name, last_name, email='', cnx=None):
    name = last_name + '|' + first_name
    logger.debug('[db_util][get_author][IN] | name: %s, email: %s', name, email)
    cursor = cnx.cursor(buffered=True, dictionary=True)
    sql = "SELECT * FROM sciencedirect.authors WHERE name = %s OR email = %s LIMIT 1"

    cursor.execute(sql, (name, email))
    fetch_res = cursor.fetchone()
    cursor.reset()
    return fetch_res


def get_article(pii, cnx=None):
    logger.debug('[databaseUtil][get_article][IN] | pii : %s', pii)
    cursor = cnx.cursor(buffered=True, dictionary=True)
    sql = "SELECT * FROM sciencedirect.articles WHERE pii = %s LIMIT 1"

    cursor.execute(sql, (pii,))
    fetch_res = cursor.fetchone()
    cursor.reset()
    logger.debug('[databaseUtil][get_article][OUT] | fetch_res : %s', fetch_res)
    return fetch_res
