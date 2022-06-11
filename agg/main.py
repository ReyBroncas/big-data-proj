import argparse
import datetime
from datetime import timedelta

from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import logging

CASSANDRA_HOST = 'cassandra'
CASSANDRA_PORT = 9042
MONGO_HOST = 'mongo'
MONGO_PORT = 27017
MONGO_USER = 'root'
MONGO_PASSWORD = 'example'


# logging.basicConfig(level=logging.DEBUG)


def parse_command_line_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--stat', type=int)
    return parser.parse_args()


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


def aggregate_1():
    cassandra_client = get_cassandra_client()
    cassandra_client.row_factory = dict_factory

    time_start, time_end = get_time_interval(60)
    query = "select domain, count(*) from table_1 " \
            f"where date>'{time_start.strftime('%Y-%m-%d %H:%M:%S')}' " \
            f"and date<'{time_end.strftime('%Y-%m-%d %H:%M:%S')}' " \
            f"and is_bot=0 " \
            f"group by domain " \
            f"allow filtering"

    records = cassandra_client.execute(query)
    statistic = {'time_start': time_start.hour,
                 'time_end': time_end.hour,
                 'statistics': records.current_rows}

    mongo_client = get_mongo_client()
    mongo_client['statistics']['agg1'].insert_one(statistic)
    print(f'[aggregator]: added new statistic 1 {statistic}')


def aggregate_2():
    time_start, time_end = get_time_interval(60 * 6)

    mongo_client = get_mongo_client()
    res = mongo_client['statistics']['agg1'].find({
        "time_start": {"$gte": time_start.hour},
        "time_end": {"$lte": time_end.hour}
    }, {})

    statistics = dict()
    for hour_stat in res:
        for entry in hour_stat['statistics']:
            domain, count = entry['domain'], entry['count']

            if not statistics.get(domain):
                statistics[domain] = count
            else:
                statistics[domain] += count

    mongo_client['statistics']['agg2'].insert_one({
        'time_start': time_start.hour,
        'time_end': time_end.hour,
        'statistics': [{k: v} for k, v in statistics.items()]
    })
    print(f'[aggregator]: added new statistic 2  {[{k: v} for k, v in statistics.items()]}')


def aggregate_3():
    cassandra_client = get_cassandra_client()
    cassandra_client.row_factory = dict_factory

    time_start, time_end = get_time_interval(60)
    query = "select domain, count(*) from table_1 " \
            f"where date>'{time_start.strftime('%Y-%m-%d %H:%M:%S')}' " \
            f"and date<'{time_end.strftime('%Y-%m-%d %H:%M:%S')}' " \
            f"and is_bot=1 " \
            f"group by domain " \
            f"allow filtering"

    records = cassandra_client.execute(query)
    res = [{'domain': item['domain'], 'created_by_bots': item['count']} for item in records.current_rows]
    statistic = {'time_start': time_start.hour,
                 'time_end': time_end.hour,
                 'statistics': res}

    mongo_client = get_mongo_client()
    mongo_client['statistics']['agg3'].insert_one(statistic)
    print(f'[aggregator]: added new statistic 3 {statistic}')


def aggregate_4():
    cassandra_client = get_cassandra_client()
    cassandra_client.row_factory = dict_factory

    time_start, time_end = get_time_interval(60 * 6)
    query = "select user_id, user_name, page_title, count(*) from table_2 " \
            f"where date>'{time_start.strftime('%Y-%m-%d %H:%M:%S')}' " \
            f"and date<'{time_end.strftime('%Y-%m-%d %H:%M:%S')}' " \
            f"group by user_id " \
            f"allow filtering"

    records = cassandra_client.execute(query)

    statistics = records.current_rows
    statistics.sort(key=lambda x: x['count'], reverse=True)

    mongo_client = get_mongo_client()
    mongo_client['statistics']['agg4'].insert_one(
        {
            'time_start': time_start.hour,
            'time_end': time_end.hour,
            'statistics': statistics[:20]
        }
    )
    print(f'[aggregator]: added new statistic 4 {statistics[:20]}')


if __name__ == '__main__':
    aggregate_4()
    args = parse_command_line_arguments()
    if args.stat == 1:
        aggregate_1()
    elif args.stat == 2:
        aggregate_2()
    elif args.stat == 3:
        aggregate_3()
    elif args.stat == 4:
        aggregate_4()
