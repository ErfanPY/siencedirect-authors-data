#!/usr/bin/env python3
import logging

from flask import (Flask, jsonify)


from get_sd_ou import databaseUtil
from get_sd_ou.databaseUtil import (init_db, get_status)

logger = logging.getLogger('mainLogger')
app = Flask(__name__)
app.config['SECRET_KEY'] = 'top test that sad test it is a secret secret!'


_db_connection = None


def db_connection():
    global _db_connection
    if db_connection is not None:
        _db_connection = init_db()
    return _db_connection


@app.route('/journals/status')
def get_db_status():
    status = get_status(cnx=db_connection())
    return jsonify(status)



if __name__ == "__main__":
    app.run(host='0.0.0.0')

