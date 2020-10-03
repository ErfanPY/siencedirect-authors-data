import mysql.connector
try:
  from .__init__ import *
except ImportError:
  from get_sd_ou.__init__ import *
logger = logging.getLogger('mainLogger')

_database = None
_cursor = None
# database.article.insert = database.tables.article.insert

def main():
  global _database
  _database = init_database()
  insert_article_data('123123alibaba', ['Erfan Gh','Abas ali mamad','sogra kobra','gholi moli'])

def init_database():
  global _database
  global _cursor
  if not _database:
    _database = mysql.connector.connect(
      host="db",
      user="root",
      password="root",
      port= '3306',
      database="siencedirect"
    )
  _cursor = _database.cursor()
  return _database

### INSERT

def insert_article(pii, title='', database=None):
  database  = database if database else init_database()
  #update = UPDATE articles SET title=%S
  sql = "INSERT INTO articles (pii, title) VALUES (%s, %s);"
  val = (pii, title)
  _cursor.execute(sql, val)
  _database.commit()
  article_id = _cursor.lastrowid
  logger.debug('[ database ] article inserted | pii: %s  id: %s', pii, article_id)
  return article_id

def insert_author(name, email='', id='', mendely='', scopus='', affiliation='', database=None):
  database  = database if database else init_database()
  sql = "INSERT INTO authors (name) VALUES (%s)"
  val = (name, )
  _cursor.execute(sql, val)
  _database.commit()
  author_id = _cursor.lastrowid
  logger.debug('[ database ] author inserted | name: %s  id: %s', name, author_id)
  return author_id

def connect_article_author(article_id, author_id, is_corresponde=0, database=None):
  database  = database if database else init_database()
  #TODO connect article with pii (get article id from articles from pii)
  sql = "INSERT INTO article_authors (article_id, author_id, is_corresponde) VALUES (%s, %s, %s);"
  val = (article_id, author_id, is_corresponde)
  _cursor.execute(sql, val)
  _database.commit()
  logger.debug('[ database ] article and author connected | article_id: %s  author_id: %s', article_id, author_id)

def insert_multi_author(authors_name_list, database=None):
  database  = database if database else init_database()
  authors_id = []
  for author_name in authors_name_list :
    authors_id.append(insert_author(author_name))
  return authors_id

def connect_multi_article_authors(article_id, authors_id_list, database=None):
  database  = database if database else init_database()
  for author_id in authors_id_list :
    connect_article_author(article_id, author_id)
  
def insert_article_data(pii, authors_name_list, database=None):
  database  = database if database else init_database()
  article_id = insert_article(pii=pii)
  
  authors_id = insert_multi_author(authors_name_list)
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

if __name__ == "__main__":
  main()


class Database():
  def __init__(self, host,  user, passwrord, dbname, **kwargs):
    self.database = mysql.connector.connect(
      host=host,
      user=user,
      password=passwrord,
      database=dbname
    )
    self.tables = {}
    self.dbname = dbname
  
  def _is_table_in_database(self, table_name):
    check = "SELECT count(*) FROM information_schema.TABLES WHERE (TABLE_SCHEMA = '%s') AND (TABLE_NAME = '%s');"
    self._cursor.execute(check, (self.dbname, table_name))
    result = self._cursor.fetch()
    return bool(result)

  def get_table(self, table_name):
    if not table_name in self.tables: 
      if not self._is_table_in_database(table_name):
        return f'{table_name} is not in database'
      self.table[table_name] = Table(self.database, table_name)
    
    return self.tables[table_name]

  def create_table(self, table_name, **columns):
    raise NotImplementedError

class Table():
  def __init__(self, database, table_name, **kwargs):
    self.table_name = table_name
    self.database = database
  
  def insert(self, title):
    sql = "INSERT INTO %s (%s, %s) VALUES (%s, %s);"
    val = (self.table_name, title)
    res = self._cursor.execute(sql, val)
    self._database.commit()
    
  def selecet():
    pass
  def is_row_exist(self, **kwargs):
    column = kwargs.keys()[0]
    value = kwargs[column]
    sql = "SELECT EXISTS(SELECT 1 FROM %s WHERE %s='%s' LIMIT 1)"
    val = (self.table_name, column, value)
    self._cursor.execute(sql, val)
    result = self._cursor.fetch()
    return result

class ArticleTable(Table):
  def __init__(self):
    super().__init__(table_name='articles')
  
  def article(self, pii):
    if not self.is_row_exist(pii=pii):
      self.insert(pii=pii)
    return self.select(pii=pii)
  
  def is_row_exist(self, **kwargs):
    return super().is_row_exist()

class AuthorTable(Table):
  def __init__(self, name):
    super().__init__(table_name='authors')
