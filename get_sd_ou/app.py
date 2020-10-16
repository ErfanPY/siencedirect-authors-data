#!/usr/bin/env python3
from flask import (Flask, request, render_template, session, flash,
                   redirect, url_for, jsonify)

from flask_wtf import FlaskForm
from wtforms import StringField, IntegerField, SubmitField

from celery import Celery
import time
import logging
from get_sd_ou import get_sd_ou
logger = logging.getLogger('mainLogger')
logger.debug('[app] INIT')
app = Flask(__name__)
app.config['SECRET_KEY'] = 'top top secret!'

app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'
app.config['CELERY_IGNORE_RESULT'] = False

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])

celery.conf.update(app.config)


class StartForm(FlaskForm):
    start_year = StringField('years')
    term = StringField('Search term')
    pub_title = StringField('in jornal or book title ')
    authors = StringField('authors')
    affiliation = StringField('affiliation')
    volume = IntegerField('volume')
    issue = IntegerField('issue')
    page = IntegerField('page')
    keywords = StringField('Title, abstract or author-specified keywords')
    title = StringField('title')
    refrence = StringField('refrences')
    issn = StringField('ISSN or ISBN')
    submit = SubmitField('Start')

@app.route('/scopus_search', methods=['POST'])
def scopus_search():
    task = get_sd_ou.scopus_search.apply_async(queue="scopus_search")
    return jsonify({}), 202, {'Location': url_for('scopus_status',
                                                  task_id=task.id)}

@app.route('/scopus_status/<task_id>')
def scopus_status(task_id):
    task = get_sd_ou.scopus_search.AsyncResult(task_id)

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


@app.route('/longtask', methods=['POST'])
def longtask():

    form = StartForm()
    logger.debug('[app] starting task')
    kwargs = {
        'date': form.start_year.data,
        'qs': form.term.data,
        'pub': form.pub_title.data,
        'authors': form.authors.data,
        'affiliations': form.affiliation.data,
        'volume': form.volume.data,
        'issue': form.issue.data,
        'page': form.page.data,
        'tak': form.keywords.data,
        'title': form.title.data,
        'refrences': form.refrence.data,
        'docId': form.issn.data
    }
    task = get_sd_ou.start_search.apply_async(kwargs=kwargs, queue="main_search")
    print(kwargs)
    return jsonify({}), 202, {'Location': url_for('taskstatus',
                                                  task_id=task.id)}


@app.route('/', methods=['GET', 'POST'])
def index():
    form = StartForm()
    return render_template('index.html', form=form)


@app.route('/taskstatus/<task_id>')
def taskstatus(task_id):
    task = get_sd_ou.start_search.AsyncResult(task_id)

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


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
