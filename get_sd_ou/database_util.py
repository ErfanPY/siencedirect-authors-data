import mysql.connector
try:
  from .__init__ import *
except ImportError:
  from get_sd_ou.__init__ import *
logger = logging.getLogger('mainLogger')

_database = None

class Database():
  def __init__(self, host,  user, passwrord, dbname, **kwargs):
    self.database = mysql.connector.connect(
      host=host,
      user=user,
      password=passwrord,
      database=dbname
    )


class Table(Database):
  def __init__(self, table_name, **kwargs):
    super().__init__(host)
    self.table_name = table_name
  def insert():
    pass
  def selecet():
    pass
  def is_row_exist(self, **kwargs):
    column = kwargs.keys()[0]
    val = kwargs[column]
    sql = "SELECT EXISTS(SELECT 1 FROM %s WHERE %s='%s' LIMIT 1)"
    val = (self.table_name, column, value)
    database.cursor.execute(sql, val)
    result = database.cursor.fetch()
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


def init_database():
  global _database
  if not _database:
    _database = mysql.connector.connect(
      host="localhost",
      user="root",
      password="123456",
      database="siencedirect"
    )
  return _database

### INSERT

def insert_article(database, pii, title=None):
  database  = database if database else init_database()
  #update = UPDATE articles SET title=%S
  sql = "INSERT INTO articles (pii, title) VALUES (%s, %s);"
  val = (pii, title)
  database.cursor.execute(sql, val)
  database.commit()

def insert_author(database, name, email='', id='', mendely='', scopus='', affiliation=''):
  database  = database if database else init_database()
  raise NotImplementedError
  """
  sql = "INSERT INTO authors (name) VALUES (%s);"
  val = (name, )
  database.cursor.execute(sql, val)
  database.commit()
  """
def connect_article_authors(database, article_id, author_id, is_coresponde=False):
  database  = database if database else init_database()
  #TODO connect article with pii (get article id from articles from pii)
  sql = "INSERT INTO article_authors (article_id, author_id) VALUES (%s, %s);"
  val = (article_id, author_id)
  database.cursor.execute(sql, val)

def insert_multi_author(database, authors):
  database  = database if database else init_database()
  #multi insert
  sql = "INSERT INTO authors (name, email) VALUES (%s, %s)"
  val = [(author.name, '') for author in authors]

  database.cursor.executemany(sql, val)

def insert_article_data(database, pii, authors):
  database  = database if database else init_database()
  article_id = insert_article(pii=pii)
  
  for author in authors:
    name = author.name
    author_id = insert_author(database, name)
    connect_article_authors(article_id, author_id, author.is_coresponde)
  sql = ""
  database

### UPDATE

def update_article():
  raise NotImplementedError

def update_author(database,):
  database  = database if database else init_database()
  raise NotImplementedError

### SELECT

def is_row_exist(database, table, column, value):
  database  = database if database else init_database()
  sql = "SELECT EXISTS(SELECT 1 FROM %s WHERE %s='%s' LIMIT 1)"
  val = (table, column, value)
  database.cursor.execute(sql, val)
  result = database.cursor.fetch()
  return result

def get_article_authors(database, article_id):
  database  = database if database else init_database()
  sql = "SELECT t1.title, t3.name\
          FROM articles AS t1\
          JOIN article_authors AS t2 ON t1.article_id = t2.article_id\
          JOIN authors AS t3 ON t2.author_id = t3.author_id\
          WHERE t2.author_id = %s"

  val = (article_id, )
  database.cursor.execute(sql, val)

  myresult = database.cursor.fetchall()
  return myresult

def get_articles_of_author(database, sql, val):
  database  = database if database else init_database()
  database.cursor.execute(sql, val)
