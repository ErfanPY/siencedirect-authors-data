import mysql.connector
from .__init__ import *
logger = logging.getLogger('mainLogger')

def init_database():
  mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="123456",
    database="siencedirect"
  )
  return mydb

def add_article(database, pii, title=None):
  #update = UPDATE articles SET title=%S
  sql = "INSERT INTO articles (pii, title) VALUES (%s, %s);"
  val = (pii, title)
  database.cursor.execute(sql, val)
  database.commit()

def update_article():
  raise NotImplementedError

def add_author(database, name, email='', id='', mendely='', scopus='', affiliation=''):
  raise NotImplementedError
  """
  sql = "INSERT INTO authors (name) VALUES (%s);"
  val = (name, )
  database.cursor.execute(sql, val)
  database.commit()
  """

def update_author():
  raise NotImplementedError

def connect_article_authors(database, article_id, author_id, is_coresponde=False):
  #TODO connect article with pii (get article id from articles from pii)
  sql = "INSERT INTO article_authors (article_id, author_id) VALUES (%s, %s);"
  val = (article_id, author_id)
  database.cursor.execute(sql, val)

def insert_article_data(database):
  pass

def get_article_authors(database, article_id):
  sql = "SELECT t1.title, t3.name\
          FROM articles AS t1\
          JOIN article_authors AS t2 ON t1.article_id = t2.article_id\
          JOIN authors AS t3 ON t2.author_id = t3.author_id\
          WHERE t2.author_id = %s"

  val = (article_id, )
  database.cursor.execute(sql, val)

  myresult = database.cursor.fetchall()
  return myresult

def add_article_data(database, article_data):
  article_id = add_article(*article_data['article'])
  for author in article_data['authors']:
    author_id = add_author(*author)
    connect_article_authors(article_id, author_id, author.is_coresponde)

def get_articles_of_author(database, sql, val):
  database.cursor.execute(sql, val)


def multi_inser(database):
  #multi insert
  sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
  val = [
    ('Peter', 'Lowstreet 4'),
    ('Amy', 'Apple st 652'),
    ('Hannah', 'Mountain 21'),
    ('Michael', 'Valley 345'),
    ('Sandy', 'Ocean blvd 2'),
    ('Betty', 'Green Grass 1'),
    ('Richard', 'Sky st 331'),
    ('Susan', 'One way 98'),
    ('Vicky', 'Yellow Garden 2'),
    ('Ben', 'Park Lane 38'),
    ('William', 'Central st 954'),
    ('Chuck', 'Main Road 989'),
    ('Viola', 'Sideway 1633')
  ]

  database.cursor.executemany(sql, val)
