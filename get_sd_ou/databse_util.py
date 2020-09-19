import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="123456",
  database="siencedirect"
)

mycursor = mydb.cursor()

def add_article(pii, title=None):
  #update = UPDATE articles SET title=%S
  sql = "INSERT INTO articles (pii, title) VALUES (%s, %s);"
  val = (pii, title)
  mycursor.execute(sql, val)
  mydb.commit()

def add_author(name):
  sql = "INSERT INTO authors (name) VALUES (%s);"
  val = (name, )
  mycursor.execute(sql, val)
  mydb.commit()

def connect_article_authors(article_id, author_id):
  #TODO connect article with pii (get article id from articles from pii)
  sql = "INSERT INTO article_authors (article_id, author_id) VALUES (%s, %s);"
  val = (article_id, author_id)
  mycursor.execute(sql, val)

def get_article_authors(article_id):
  sql = "SELECT t1.title, t3.name\
          FROM articles AS t1\
          JOIN article_authors AS t2 ON t1.article_id = t2.article_id\
          JOIN authors AS t3 ON t2.author_id = t3.author_id\
          WHERE t2.author_id = %s"

  val = (article_id, )
  mycursor.execute(sql, val)

  myresult = mycursor.fetchall()
  return myresult

def initiate_database(cursor, ):
  cursor.execute(sql, val)

def get_articles_of_author(cursor, ):
  cursor.execute(sql, val)


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

mycursor.executemany(sql, val)