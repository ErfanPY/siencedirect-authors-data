from flask import Flask, Response
from .__init__ import *
from get_sd_ou.get_sd_ou import *

app = Flask(__name__)

search_thread = None

@app.route('/start')
def start ():
   global search_thread
   search_thread = threading.Thread(target=start_search, args=(2018,))
   search_thread.start()
   return 'Started'

@app.route('/stop')
def stop ():
   global search_thread
   search_thread.

@app.route('/status')
def status ():
   with open('get_sd_ou/get_sd_ou.log', 'r') as f:
      content = f.readlines()[::-1]
   return Response(content, mimetype='text/plain')

@app.route('/article/<pii>')
def article_data(pii):
   return f"Article data {pii} "

@app.route('/article/<name>')
def author_data(name):
   pass

if __name__ == '__main__':
   app.run()