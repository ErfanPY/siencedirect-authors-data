import mysql.connector as co
cnx = co.connect(host='localhost', user='sciencedirect', password='root', port='3306', database='sciencedirect')
cursor = cnx.cursor(buffered=True)
cursor.execute('SELECT name FROM sciencedirect.authors WHERE scopus is NULL')
print(cursor.fetchmany(10))
