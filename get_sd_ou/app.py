#!/usr/bin/env python3
from flask import (Flask, request, render_template, session, flash,
                   redirect, url_for, jsonify)

from flask_wtf import FlaskForm
from wtforms import StringField, IntegerField, SubmitField

from celery import Celery
import time
import logging
from get_sd_ou import get_sd_ou
from get_sd_ou.database_util import get_search_suggest, get_search, get_db_result
from get_sd_ou.class_util import Search_page
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
    date = StringField('years')
    qs = StringField('Search term')
    pub = StringField('in jornal or book title ')
    authors = StringField('authors')
    affiliation = StringField('affiliation')
    volume = IntegerField('volume')
    issue = IntegerField('issue')
    page = IntegerField('page')
    tak = StringField('Title, abstract or author-specified keywords')
    title = StringField('title')
    refrences = StringField('refrences')
    docId = StringField('ISSN or ISBN')
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


@app.route('/db_search')
def db_search():
    form = StartForm()
    return render_template('db_search.html', form=form)

@app.route('/db_start_search', methods=['POST'])
def db_start_search():
    form = StartForm()
    logger.debug('[app] starting task')
    kwargs = {
        'date': form.date.data,
        'qs': form.qs.data,
        'pub': form.pub.data,
        'authors': form.authors.data,
        'affiliations': form.affiliation.data,
        'volume': form.volume.data,
        'issue': form.issue.data,
        'page': form.page.data,
        'tak': form.tak.data,
        'title': form.title.data,
        'refrences': form.refrences.data,
        'docId': form.docId.data
    }
    kwargs['offset'] = 0
    database_result = get_db_result(**kwargs)

    return render_template('db_result.html', searchs=database_result)
    #return str(database_result)

@app.route('/db_suggest', methods=['POST'])
def db_suggest():
    logger.debug('[app][db_suggest][IN]')
    print(request.data)
    inp = request.form.to_dict()
    input_key = inp['key']
    input_value = inp['value']
    print('###', input_key, input_value)
    suggestion_list = get_search_suggest(input_key, input_value)

    logger.debug('[app][db_suggest][OUT] | suggestion_list : %s', suggestion_list)
    return jsonify({'suggestion_list':suggestion_list})


@app.route('/db_suggest_all', methods=['POST'])
def db_suggest_all():
    logger.debug('[app][db_suggest][IN]')
    form = dict(request.form)
    del form['csrf_token']
    suggests = get_search_suggest(**form)
    res = {}
    for key in form:
        res[key] = []
    for suggest in suggests:
        for key, value in suggest.items():
            form_value = form.get(key, False)
            print('db_sugg####', key, "|", value, '|', not form_value,
                  "|", form_value == ' ', "|", key in form)
            if value and key in form and value != form_value and value != ' ':
                res[key].append(value)

    logger.debug('[app][db_suggest][OUT] | res : %s', res)
    return jsonify(res)


@app.route('/longtask', methods=['POST'])
def longtask():

    form = StartForm()
    logger.debug('[app] starting task')
    kwargs = {
        'date': form.date.data,
        'qs': form.qs.data,
        'pub': form.pub.data,
        'authors': form.authors.data,
        'affiliations': form.affiliation.data,
        'volume': form.volume.data,
        'issue': form.issue.data,
        'page': form.page.data,
        'tak': form.tak.data,
        'title': form.title.data,
        'refrences': form.refrences.data,
        'docId': form.docId.data
    }
    kwargs['offset'] = 0
    task = get_sd_ou.start_search.apply_async(
        kwargs=kwargs, queue="main_search")
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
