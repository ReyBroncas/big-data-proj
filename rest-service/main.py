import json

from flask import Flask
from flask import request
from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import datetime
from datetime import timedelta

app = Flask(__name__)

CASSANDRA_HOST = 'localhost'
CASSANDRA_PORT = 9040
MONGO_HOST = 'localhost'
MONGO_PORT = 27017
MONGO_USER = 'root'
MONGO_PASSWORD = 'example'


def get_cassandra_client():
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    return cluster.connect('big_data_project')


def get_mongo_client():
    return MongoClient(MONGO_HOST, MONGO_PORT, username=MONGO_USER, password=MONGO_PASSWORD)


def get_time_interval(offset):
    time_now = datetime.datetime.now()
    time_end = time_now - timedelta(minutes=time_now.minute, seconds=time_now.second)
    time_start = time_end - timedelta(minutes=offset)
    return time_start, time_end


@app.route("/created_pages")
def created_pages():
    if request.args.get('is_bot'):
        db = mongo_client['statistics']['agg2']
    else:
        db = mongo_client['statistics']['agg3']

    ret = list(db.find({}, {}).sort())
    statistics = [x for item in ret for x in item['statistics']]

    res = {
        'time_start': ret[0]['time_start'],
        'time_end': ret[0]['time_end'],
        'statistics': statistics
    }
    return json.dumps(res)


@app.route("/top_users")
def top_users():
    db = mongo_client['statistics']['agg4']

    ret = list(db.find({}, {}))
    statistics = [x for item in ret for x in item['statistics']]

    res = {
        'time_start': ret[0]['time_start'],
        'time_end': ret[0]['time_end'],
        'statistics': statistics
    }
    return json.dumps(res)


@app.route('/existing_domains')
def domains():
    cassandra_client = get_cassandra_client()
    cassandra_client.row_factory = dict_factory

    query = "select domain from table_1 " \
            f"group by domain " \
            f"allow filtering"

    records = cassandra_client.execute(query)
    records
    return json.dumps([_['domain'] for _ in records.current_rows])


@app.route('/pages_by_user')
def pages():
    id = request.args.get('user_id')
    cassandra_client = get_cassandra_client()
    cassandra_client.row_factory = dict_factory

    query = "select page_title from table_2 " \
            f"group by user_id " \
            f"allow filtering"

    records = cassandra_client.execute(query)

    return json.dumps([_['page_title'] for _ in records.current_rows])


@app.route('/articles_by_domain')
def articles_by_domain():
    domain = request.args.get('domain')
    cassandra_client = get_cassandra_client()
    cassandra_client.row_factory = dict_factory

    query = "select page_title from table_1 " \
            f"where domain='{domain}' " \
            f"allow filtering"

    records = cassandra_client.execute(query)
    print(records)
    return json.dumps([_['page_title'] for _ in records.current_rows])


@app.route('/page_by_id')
def articles_by_domain():
    domain = request.args.get('domain')
    cassandra_client = get_cassandra_client()
    cassandra_client.row_factory = dict_factory

    query = "select page_title from table_1 " \
            f"where domain='{domain}' " \
            f"allow filtering"

    records = cassandra_client.execute(query)
    print(records)
    return json.dumps([_['page_title'] for _ in records.current_rows])


if __name__ == '__main__':
    mongo_client = get_mongo_client()
    app.run(debug=True, host="0.0.0.0", port=3000)
