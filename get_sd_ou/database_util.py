import mysql.connector
import logging

logger = logging.getLogger('mainLogger')


database = mysql.connector.connect(
    host="localhost",
    user="sciencedirect",
    password="root",
    port='3306'
)

#executeScriptsFromFile('/root/dev/sciencedirect-authors-data/db/scripts/sciencedirect.sql', database.cursor)
database.commit()

# INSERT


def insert_article(pii, title=''):
    # update = UPDATE articles SET title=%S
    sql = "INSERT IGNORE INTO sciencedirect.articles (pii, title) VALUES (%s, %s);"
    val = (pii, title)
    database.cursor.execute(sql, val)
    database.commit()
    article_id = database.cursor.lastrowid
    logger.debug(
        '[ database ] article inserted | pii: %s  id: %s', pii, article_id)
    return article_id


def insert_author(first_name, last_name, email='', affiliation='', is_coresponde=False, id=None):
    name = last_name+'|'+first_name
    sql = "INSERT IGNORE INTO sciencedirect.authors (name, email, affiliation, scopus) \
            VALUES (%s, %s, %s, %s)"

    val = (name, email, affiliation, id)
    database.cursor.execute(sql, val)
    database.commit()
    author_id = database.cursor.lastrowid
    logger.debug(
        '[ database ] author inserted | name: %s  id: %s', name, author_id)
    return author_id


def connect_article_author(article_id, author_id, is_corresponde=0):
    # TODO connect article with pii (get article id from articles from pii)
    sql = "INSERT IGNORE INTO sciencedirect.article_authors (article_id, author_id, is_corresponde) VALUES (%s, %s, %s);"
    val = (article_id, author_id, is_corresponde)
    database.cursor.execute(sql, val)
    database.commit()
    logger.debug(
        '[ database ] article and author connected | article_id: %s  author_id: %s', article_id, author_id)


def insert_search():
    raise NotImplementedError


def connect_search_article():
    raise NotImplementedError


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

# UPDATE


def update_article():
    raise NotImplementedError


def update_author_scopus(name, id):
    sql = 'UPDATE sciencedirect.authors SET scopus=%s WHERE name=%s LIMIT 1;'
    val = (id, name)
    database.cursor.execute(sql, val)
    database.commit()
    author_id = database.cursor.lastrowid
    return author_id


# SELECT


def is_row_exist(table, column, value):
    sql = "SELECT EXISTS(SELECT 1 FROM %s WHERE %s='%s' LIMIT 1)"
    val = (table, column, value)
    database.cursor.execute(sql, val)
    result = database.cursor.fetch()
    return result


def get_id_less_authors():
    sql = "SELECT name FROM sciencedirect.authors WHERE scopus is NULL"
    database.cursor.execute(sql)
    names = database.cursor.fetchall()
    res = []
    for name in names:
        last, first = name.split('|')
        res.append({'last_name': last, 'first_name': first})
    return res


def get_article_authors(article_id):
    sql = "SELECT t1.title, t3.name\
          FROM articles AS t1\
          JOIN article_authors AS t2 ON t1.article_id = t2.article_id\
          JOIN authors AS t3 ON t2.author_id = t3.author_id\
          WHERE t2.author_id = %s"

    val = (article_id, )
    database.cursor.execute(sql, val)

    myresult = database.cursor.fetchall()
    return myresult


def get_articles_of_author(sql, val):
    database.cursor.execute(sql, val)


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
