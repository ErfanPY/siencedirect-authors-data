#!/usr/bin/env python3
import json
import logging

from flask import (Flask, jsonify, make_response,
                   render_template, request, send_file)


from get_sd_ou import get_sd_ou, databaseUtil
from get_sd_ou.databaseUtil import (get_db_result, get_search_suggest, init_db)

logger = logging.getLogger('mainLogger')
logger.debug('[app] INIT')
app = Flask(__name__)
app.config['SECRET_KEY'] = 'top top secret!'


_db_connection = None


def db_connection():
    global _db_connection
    if db_connection is not None:
        _db_connection = init_db()
    return _db_connection


@app.route('/db_search')
def db_search():
    return render_template('db_search.html')


@app.route('/db_start_search', methods=['POST'])
def db_start_search():
    logger.debug('[app] starting task')
    kwargs = dict(request.form)
    kwargs['offset'] = 0
    database_result = get_db_result(cnx=db_connection(), **kwargs)
    
    logger.debug('[app][db_start_search] | database_result: %s ', str(database_result)[:20])
    
    return render_template('db_result.html', searchs=database_result)


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
    _ = '<a href="/return-files/" target="blank"><button>Download!</button></a>'
    return send_file('/var/www/PythonProgramming/PythonProgramming/static/images/python.jpg',
                     attachment_filename='python.jpg')


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

@app.route('/journals/status')
def get_status():
    status = databaseUtil.get_status(cnx=db_connection())
    return jsonify(status)



if __name__ == "__main__":
    app.run(host='0.0.0.0')

