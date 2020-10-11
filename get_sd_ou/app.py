import os
import random
import time
from flask import Flask, request, render_template, session, flash, redirect, \
    url_for, jsonify
from flask_mail import Mail, Message
from celery import Celery


app = Flask(__name__)
app.config['SECRET_KEY'] = 'top-secret!'

# Flask-Mail configuration
app.config['MAIL_SERVER'] = 'smtp.googlemail.com'
app.config['MAIL_PORT'] = 587
app.config['MAIL_USE_TLS'] = True
app.config['MAIL_USERNAME'] = os.environ.get('MAIL_USERNAME')
app.config['MAIL_PASSWORD'] = os.environ.get('MAIL_PASSWORD')
app.config['MAIL_DEFAULT_SENDER'] = 'flask@example.com'

# Celery configuration
app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'


# Initialize extensions
mail = Mail(app)

# Initialize Celery
celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)


@celery.task
def send_async_email(email_data):
    """Background task to send an email with Flask-Mail."""
    msg = Message(email_data['subject'],
                  sender=app.config['MAIL_DEFAULT_SENDER'],
                  recipients=[email_data['to']])
    msg.body = email_data['body']
    with app.app_context():
        mail.send(msg)


@celery.task(bind=True)
def long_task(self):
    """Background task that runs a long function with progress reports."""
    verb = ['Starting up', 'Booting', 'Repairing', 'Loading', 'Checking']
    adjective = ['master', 'radiant', 'silent', 'harmonic', 'fast']
    noun = ['solar array', 'particle reshaper', 'cosmic ray', 'orbiter', 'bit']
    message = ''
    total = random.randint(10, 50)
    for i in range(total):
        if not message or random.random() < 0.25:
            message = '{0} {1} {2}...'.format(random.choice(verb),
                                              random.choice(adjective),
                                              random.choice(noun))
        self.update_state(state='PROGRESS',
                          meta={'current': i, 'total': total,
                                'status': message})
        time.sleep(1)
    return {'current': 100, 'total': 100, 'status': 'Task completed!',
            'result': 42}


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'GET':
        return render_template('index.html', email=session.get('email', ''))
    email = request.form['email']
    session['email'] = email

    # send the email
    email_data = {
        'subject': 'Hello from Flask',
        'to': email,
        'body': 'This is a test email sent from a background Celery task.'
    }
    if request.form['submit'] == 'Send':
        # send right away
        send_async_email.delay(email_data)
        flash('Sending email to {0}'.format(email))
    else:
        # send in one minute
        send_async_email.apply_async(args=[email_data], countdown=60)
        flash('An email will be sent to {0} in one minute'.format(email))

    return redirect(url_for('index'))


@app.route('/longtask', methods=['POST'])
def longtask():
    task = long_task.apply_async()
    return jsonify({}), 202, {'Location': url_for('taskstatus',
                                                  task_id=task.id)}


@app.route('/status/<task_id>')
def taskstatus(task_id):
    task = long_task.AsyncResult(task_id)
    if task.state == 'PENDING':
        response = {
            'state': task.state,
            'current': 0,
            'total': 1,
            'status': 'Pending...'
        }
    elif task.state != 'FAILURE':
        response = {
            'state': task.state,
            'current': task.info.get('current', 0),
            'total': task.info.get('total', 1),
            'status': task.info.get('status', '')
        }
        if 'result' in task.info:
            response['result'] = task.info['result']
    else:
        # something went wrong in the background job
        response = {
            'state': task.state,
            'current': 1,
            'total': 1,
            'status': str(task.info),  # this is the exception raised
        }
    return jsonify(response)


if __name__ == '__main__':
    app.run(debug=True)

#-_-_-_-_-_-_-_-_-_-_-_-_-_-_-__-_-_-_-_-_-_-_-_-_-_-_-_-_

# from flask import Flask, Response, redirect, render_template, flash

# from flask_wtf import FlaskForm
# from wtforms import StringField, SubmitField, IntegerField
# from wtforms.validators import DataRequired
# from flask_socketio import SocketIO, emit

# import threading
# import logging
# from get_sd_ou.get_sd_ou import start_search

# app = Flask(__name__)
# app.config['SECRET_KEY'] = 'you-will-never-guess'
# socketio = SocketIO(app)

# logger = logging.getLogger('mainLogger')

# search_thread = None
# is_search_running = False

# class StartForm(FlaskForm):
    # term = StringField('Search term')
    # pub_title = StringField('in jornal or book title ')
    # start_year = IntegerField('years', validators=[DataRequired()])
    # authors = StringField('authors')
    # affiliation = StringField('affiliation')
    # volume = IntegerField('volume')
    # issue = IntegerField('issue')
    # page = IntegerField('page')
    # keywords = StringField('Title, abstract or author-specified keywords')
    # title = StringField('title')
    # refrence = StringField('refrences')
    # issn = StringField('ISSN or ISBN')
#    submit = SubmitField('Start')

# @app.route('/status')
# def status ():
#    if is_search_running:
#       with open('get_sd_ou/get_sd_ou.log', 'r') as f:
#          content = f.readlines()[::-1]
#       return Response(content, mimetype='text/plain')
#    return redirect('/start')


# @app.route('/start', methods=['GET', 'POST'])
# def start ():
#    global is_search_running

#    form = StartForm()
#    if form.validate_on_submit():
#       flash('Start searching from year {}, with {} affiliation'.format(
#             form.start_year.data, form.affiliation.data))
#       logger.debug('[web_ui] start ')
#       global search_thread
    #    kwargs = {
    #        'start_year':form.start_year.data, 
    #         'term':form.term.data,
    #         'pub_title':form.pub_title.data,
    #         'authors':form.authors.data,
    #         'affiliation':form.affiliation.data,
    #         'volume':form.volume.data,
    #         'issue':form.issue.data,
    #         'page':form.page.data,
    #         'keywords':form.keywords.data,
    #         'title':form.title.data,
    #         'refrence':form.refrence.data,
    #         'issn':form.issn.data
    #             }
#       search_thread = threading.Thread(target=start_search, kwargs=kwargs)
#       search_thread.start()
      
#       is_search_running = True
#       return redirect('/status')
   
#    return render_template('start.html', form=form)

# @app.route('/stop')
# def stop ():
#    global search_thread
#    del search_thread

# @app.route('/dbstat')
# def dbstat ():
#    global search_thread
#    del search_thread

# @app.route('/article/<pii>')
# def article_data(pii):
#    return f"Article data {pii} "

# @app.route('/article/<name>')
# def author_data(name):
#    pass