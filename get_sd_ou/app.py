#!/usr/bin/env python3
import logging

from flask import (Flask, jsonify, render_template, request)


from get_sd_ou import databaseUtil
from get_sd_ou.databaseUtil import (get_db_result, get_search_suggest, init_db)

logger = logging.getLogger('mainLogger')
app = Flask(__name__)
app.config['SECRET_KEY'] = 'top test that sad test it is a secret secret!'


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


@app.route('/journals/status')
def get_status():
    status = databaseUtil.get_status(cnx=db_connection())
    return jsonify(status)



if __name__ == "__main__":
    app.run(host='0.0.0.0')

