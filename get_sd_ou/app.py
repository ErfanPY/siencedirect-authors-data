#!/usr/bin/env python3
from flask import (Flask, request, render_template, session, flash,
                   redirect, url_for, jsonify)

from flask_wtf import FlaskForm
from wtforms import StringField, IntegerField, SubmitField

from celery import Celery
import time
import random
import logging
from get_sd_ou import get_sd_ou
from get_sd_ou.database_util import init_db, get_search_suggest, get_search, get_db_result
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

db_connection = init_db()

@app.route('/scopus_search', methods=['POST'])
def scopus_search():
    # task = get_sd_ou.scopus_search.apply_async(queue="scopus_search")
    # return jsonify({}), 202, {'Location': url_for('scopus_status',
    #                                               task_id=task.id)}
    return jsonify({}), 202, {'Location': url_for('scopus_status')}


scopus_i = 0
scopus_total = random.randint(10000, 30000)
scopus_time = time.time()


@app.route('/scopus_status')
def scopus_status():
    global scopus_i
    global scopus_time
    # task = get_sd_ou.scopus_search.AsyncResult(task_id)

    # if task.state == 'PENDING':
    #     response = {
    #         'state': task.state,
    #         'current': 0,
    #         'total': 1,
    #         'status': 'Pending...'
    #     }
    # elif task.state != 'FAILURE':
    #     response = {
    #         'state': task.state,
    #         'current': task.info.get('current', 0),
    #         'total': task.info.get('total', 1),
    #         'status': task.info.get('status', '')
    #     }
    #     if 'result' in task.info:
    #         response['result'] = task.info['result']
    # else:
    #     # something went wrong in the background job
    #     response = {
    #         'state': task.state,
    #         'current': 1,
    #         'total': 1,
    #         'status': str(task.info),  # this is the exception raised
    #     }

    response = {
        'state': 'PROGRESS',
        'current': scopus_i,
        'total': scopus_total,
        'status': str(scopus_i)+'/'+str(scopus_total)
    }

    if time.time()-scopus_time > 4:
        scopus_time = time.time()
        scopus_i += 1
    if scopus_i == scopus_total:
        response['result'] = str(scopus_total)+' author got'

    return jsonify(response)


@app.route('/db_search')
def db_search():
    return render_template('db_search.html')


@app.route('/db_start_search', methods=['POST'])
def db_start_search():
    logger.debug('[app] starting task')
    kwargs = dict(request.form)
    kwargs['offset'] = 0
    database_result = get_db_result(cnx=db_connection, **kwargs)

    return render_template('db_result.html', searchs=database_result)
    # return str(database_result)


@app.route('/db_suggest', methods=['POST'])
def db_suggest():
    logger.debug('[app][db_suggest][IN]')
    inp = request.form.to_dict()
    input_key = inp['key']
    input_value = inp['value']
    suggestion_list = get_search_suggest(input_key, input_value)

    logger.debug(
        '[app][db_suggest][OUT] | suggestion_list : %s', suggestion_list)
    return jsonify({'suggestion_list': suggestion_list})


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
            if value and key in form and value != form_value and value != ' ':
                res[key].append(value)

    logger.debug('[app][db_suggest][OUT] | res : %s', res)
    return jsonify(res)


@app.route('/longtask', methods=['POST'])
def longtask():
    logger.debug('[app] starting task')
    kwargs = dict(request.form)
    kwargs['offset'] = 0
    task = get_sd_ou.start_search.apply_async(kwargs=kwargs, queue="main_search")
    return jsonify({}), 202, {'Location': url_for('taskstatus', task_id=task.id)}


@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('index.html')


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

