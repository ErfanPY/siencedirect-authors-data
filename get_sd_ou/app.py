#!/usr/bin/env python3
import json
import logging
from logging import log
import random
import time

import redis
from celery import Celery
from flask import (Flask, jsonify, make_response,
                   render_template, request, url_for, send_file)


from get_sd_ou import get_sd_ou
from get_sd_ou.classUtil import SearchPage
from get_sd_ou.databaseUtil import (get_db_result, get_search,
                                     get_search_suggest, init_db)

logger = logging.getLogger('mainLogger')
logger.debug('[app] INIT')
app = Flask(__name__)
app.config['SECRET_KEY'] = 'top top secret!'

app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'
app.config['CELERY_IGNORE_RESULT'] = False

app.config['BROKER_CONNECTION_RETRY'] = True
app.config['BROKER_CONNECTION_MAX_RETRIES'] = 0
app.config['BROKER_CONNECTION_TIMEOUT'] = 120

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])

celery.conf.update(app.config)

redisClient = redis.StrictRedis(host='localhost', port=6379, db=0)

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
    
    logger.debug('[app][db_start_search] | database_result: %s ', str(database_result)[:20])
    
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

@app.route('/download_db')
def download_db():
    _  = '<a href="/return-files/" target="blank"><button>Download!</button></a>'
    # Generate sql file (or any prefered file format)
    return send_file('/var/www/PythonProgramming/PythonProgramming/static/images/python.jpg', attachment_filename='python.jpg')

@app.route('/login', methods=['POST', 'GET'])
def login():
    html = """
    username
    password
    Login
    """
    # check db
    # add to cookie
    # redirect ro callback url if exist else to index
    return html

@app.route('/start_multi_search', methods=['POST'])
def multi_search():
    data = dict(request.form)
    search_kwargs = {}

    for item in data.get('form', '').split("&"):
        key, value = item.split('=')
        search_kwargs[key] = value

    worker_count = int(data.get('worker_count', 1))

    task = get_sd_ou.start_multi_search.apply_async(kwargs={"worker_count":worker_count, **search_kwargs}, queue="main_search")

    task_id_list = task.get()

    cookie_data = request.cookies.get('task_info_list')
    prev_task_info_list = [] if not cookie_data else json.loads(cookie_data)
    [prev_task_info_list.append(f'{task_id}|{json.dumps(data)}') for task_id in task_id_list]

    resp = make_response()
    resp.set_cookie('task_info_list', json.dumps(prev_task_info_list))
    return resp

def start_task(**search_kwargs):
    task = get_sd_ou.start_search.apply_async(kwargs=search_kwargs, queue="main_search")
    
    cookie_data = request.cookies.get('task_info_list')
    prev_task_info_list = [] if not cookie_data else json.loads(cookie_data)
    prev_task_info_list.append(f'{task.id}|{json.dumps(search_kwargs)}')

    resp = make_response(jsonify({}), 202, {'Location': url_for('taskstatus', task_id=task.id), 'task_id': task.id})
    resp.set_cookie('task_info_list', json.dumps(prev_task_info_list))

    return resp

@app.route('/longtask', methods=['POST'])
def longtask():
    logger.debug('[app] starting task')
    kwargs = dict(request.form)
    kwargs['offset'] = 0
    
    response = start_task(**kwargs)
    
    return response

@app.route('/stop_task/<task_id>', methods=['POST'])
def stop_task(task_id):
    get_sd_ou.start_search.AsyncResult(task_id).revoke(terminate=True)

    redisClient.sadd("celery_revoke", task_id)
    
    cookie_data = request.cookies.get('task_info_list')
    prev_task_info_list = [] if not cookie_data else json.loads(cookie_data)
    [prev_task_info_list.remove(task_info) for task_info in prev_task_info_list if task_id in task_info]

    resp = make_response()
    resp.set_cookie('task_info_list', json.dumps(prev_task_info_list))
    return resp

@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('index.html')

@app.route('/update_all_searchs')
def update_all_searchs():
    task_info_list = json.loads(request.cookies.get('task_info_list', '{}'))
    return json.dumps([url_for('taskstatus', task_id=task_id.split('|')[0]) for task_id in task_info_list])

@app.route('/restart_task/<task_id>', methods=['POST'])
def restart_task(task_id):
    cookie_data = request.cookies.get('task_info_list')
    prev_task_info_list = [] if not cookie_data else json.loads(cookie_data)
    task_info = [task_info for task_info in prev_task_info_list if task_id in task_info][0]
    _, task_kwargs_json = task_info.split('|')
    task_kwargs = dict(json.loads(task_kwargs_json))
    cookie_added_response = start_task(**task_kwargs)
    return cookie_added_response

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
    cookie_data = request.cookies.get('task_info_list')
    prev_task_info_list = [] if not cookie_data else json.loads(cookie_data)
    
    form_args_json = [task_info.split('|')[-1] for task_info in prev_task_info_list if task_id in task_info]
    form_args = ''
    if form_args_json :     
        form_args_dict = {key:value for (key, value) in json.loads(form_args_json[0]).items() if value}
        form_args = ' | '.join([f'{key} : {value}' for (key, value) in form_args_dict.items()])
    response['form'] = form_args

    return jsonify(response)


if __name__ == "__main__":
    app.run(host='0.0.0.0')

