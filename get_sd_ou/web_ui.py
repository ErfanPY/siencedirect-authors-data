from flask import Flask, Response, redirect, render_template

from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired

import threading
from get_sd_ou.get_sd_ou import start_search

app = Flask(__name__)
app.config['SECRET_KEY'] = 'you-will-never-guess'

search_thread = None
is_search_running = False

class StartForm(FlaskForm):
   start_year = StringField('Start year', validators=[DataRequired()])
   affiliation = StringField('Affiliation')
   submit = SubmitField('Start')

@app.route('/start')
def start ():
   form = StartForm()
   return render_template('start.html', form=form)
   global search_thread
   search_thread = threading.Thread(target=start_search, args=(2018,))
   search_thread.start()
   

@app.route('/stop')
def stop ():
   global search_thread
   del search_thread

@app.route('/status')
def status ():
   if is_search_running:
      with open('get_sd_ou/get_sd_ou.log', 'r') as f:
         content = f.readlines()[::-1]
      return Response(content, mimetype='text/plain')
   return redirect('/start')

@app.route('/dbstat')
def dbstat ():
   global search_thread
   del search_thread

@app.route('/article/<pii>')
def article_data(pii):
   return f"Article data {pii} "

@app.route('/article/<name>')
def author_data(name):
   pass

app.run(host='0.0.0.0', debug=True)
