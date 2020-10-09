from flask import Flask, Response, redirect, render_template, flash

from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, IntegerField
from wtforms.validators import DataRequired

import threading
import logging
from get_sd_ou.get_sd_ou import start_search

app = Flask(__name__)
app.config['SECRET_KEY'] = 'you-will-never-guess'

logger = logging.getLogger('mainLogger')

search_thread = None
is_search_running = False

class StartForm(FlaskForm):
   start_year = IntegerField('Start year', validators=[DataRequired()])
   affiliation = StringField('Affiliation')
   submit = SubmitField('Start')

@app.route('/')
@app.route('/status')
def status ():
   if is_search_running:
      with open('get_sd_ou/get_sd_ou.log', 'r') as f:
         content = f.readlines()[::-1]
      return Response(content, mimetype='text/plain')
   return redirect('/start')


@app.route('/start', methods=['GET', 'POST'])
def start ():
   global is_search_running

   form = StartForm()
   if form.validate_on_submit():
      flash('Start searching from year {}, with {} affiliation'.format(
            form.start_year.data, form.affiliation.data))
      logger.debug('[web_ui] start ')
      global search_thread
      kwargs = {'start_year':form.start_year.data, 'affiliation':form.affiliation.data}
      search_thread = threading.Thread(target=start_search, kwargs=kwargs)
      search_thread.start()
      
      is_search_running = True
      return redirect('/status')
   
   return render_template('start.html', form=form)

@app.route('/stop')
def stop ():
   global search_thread
   del search_thread

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

app.run(host='0.0.0.0')
