from flask import Flask
from .__init__ import *
from get_sd_ou.get_sd_ou import *

app = Flask(__name__)

@app.route('/article/<pii>')
def article_data(pii):
   return f"Article data {pii} "

if __name__ == '__main__':
   app.run(debug=True)