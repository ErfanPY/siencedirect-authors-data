import mysql.connector
import logging

logger = logging.getLogger('mainLogger')

_database = None
_cursor = None
# database.article.insert = database.tables.article.insert

def main():
  global _database
  _database = init_database()
  executeScriptsFromFile('/root/Desktop/sciencedirect-authors-data/db/scripts/sciencedirect.sql', _cursor)
  _database.commit()

def init_database():
  global _database
  global _cursor
  if not _database:
    _database = mysql.connector.connect(
      host="localhost",
      user="sciencedirect",
      password="root",
      port= '3306'
    )
  _cursor = _database.cursor()
  executeScriptsFromFile('/root/Desktop/sciencedirect-authors-data/db/scripts/sciencedirect.sql', _cursor)
  _database.commit()

  return _database

### INSERT

def insert_article(pii, title='', database=None):
  database  = database if database else init_database()
  #update = UPDATE articles SET title=%S
  sql = "INSERT INTO sciencedirect.articles (pii, title) VALUES (%s, %s);"
  val = (pii, title)
  _cursor.execute(sql, val)
  _database.commit()
  article_id = _cursor.lastrowid
  logger.debug('[ database ] article inserted | pii: %s  id: %s', pii, article_id)
  return article_id

def insert_author(first_name, last_name, email='', id='', affiliation='', database=None):
  database  = database if database else init_database()
  name = first_name+'|'+last_name
  sql = "INSERT INTO sciencedirect.authors (name, email, id, affiliation) VALUES (%s, %s, %s, %s)"
  val = (name, email, id, affiliation)
  _cursor.execute(sql, val)
  _database.commit()
  author_id = _cursor.lastrowid
  logger.debug('[ database ] author inserted | name: %s  id: %s', name, author_id)
  return author_id

def connect_article_author(article_id, author_id, is_corresponde=0, database=None):
  database  = database if database else init_database()
  #TODO connect article with pii (get article id from articles from pii)
  sql = "INSERT INTO sciencedirect.article_authors (article_id, author_id, is_corresponde) VALUES (%s, %s, %s);"
  val = (article_id, author_id, is_corresponde)
  _cursor.execute(sql, val)
  _database.commit()
  logger.debug('[ database ] article and author connected | article_id: %s  author_id: %s', article_id, author_id)

def insert_multi_author(authors_list, database=None):
  database  = database if database else init_database()
  authors_id = []
  for author in authors_list :
    authors_id.append(insert_author(**author))
  return authors_id

def connect_multi_article_authors(article_id, authors_id_list, database=None):
  database  = database if database else init_database()
  for author_id in authors_id_list :
    connect_article_author(article_id, author_id)
  
def insert_article_data(pii, authors, database=None, **kwargs):
  database  = database if database else init_database()
  article_id = insert_article(pii=pii)
  
  authors_id = insert_multi_author(authors)
  connect_multi_article_authors(article_id, authors_id)

### UPDATE

def update_article():
  raise NotImplementedError

def update_author(database,):
  database  = database if database else init_database()
  raise NotImplementedError

### SELECT

def is_row_exist(table, column, value, database=None):
  database  = database if database else init_database()
  sql = "SELECT EXISTS(SELECT 1 FROM %s WHERE %s='%s' LIMIT 1)"
  val = (table, column, value)
  _cursor.execute(sql, val)
  result = _cursor.fetch()
  return result

def get_article_authors(article_id, database=None):
  database  = database if database else init_database()
  sql = "SELECT t1.title, t3.name\
          FROM articles AS t1\
          JOIN article_authors AS t2 ON t1.article_id = t2.article_id\
          JOIN authors AS t3 ON t2.author_id = t3.author_id\
          WHERE t2.author_id = %s"

  val = (article_id, )
  _cursor.execute(sql, val)

  myresult = _cursor.fetchall()
  return myresult

def get_articles_of_author(sql, val, database=None):
  database  = database if database else init_database()
  _cursor.execute(sql, val)

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
      print ("Command skipped: ", msg)

if __name__ == "__main__":
  main()
