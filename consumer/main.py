import json
import logging
import time

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from cassandra.cluster import Cluster
from dateutil import parser
import logging

# logging.basicConfig(level=logging.DEBUG)

CASSANDRA_HOST = 'cassandra'
CASSANDRA_PORT = 9042
KAFKA_TOPIC = 'wikipedia_events'
KAFKA_BOOTSTRAP_SERVER = 'kafka:9092'


def get_kafka_consumer():
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x)
        )

    except NoBrokersAvailable:
        print('No broker found at {}'.format(KAFKA_BOOTSTRAP_SERVER))
        raise
    if consumer.bootstrap_connected():
        print('Kafka consumer connected!')
        return consumer
    else:
        print('Failed to establish connection!')
        exit(1)


def get_cassandra_client():
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    return cluster.connect('big_data_project')


def compose_query(msg):
    val = msg.value
    date = str(parser.parse(val['timestamp']))
    domain = val['domain'].replace("'", "")
    page_title = val['page_title'].replace("'", r"")
    user_id = val['user_id']
    is_bot = int(val['is_bot'])
    user_name = val['user_name'].replace("'", "")

    return (f"INSERT INTO table_1 (date, domain, page_title, user_id, is_bot, user_name) " \
            f"VALUES ('{date}', '{domain}', '{page_title}', '{user_id}', {is_bot}, '{user_name}')",

            f"INSERT INTO table_2 (user_id, user_name, page_title, date) " \
            f"VALUES ('{user_id}', '{user_name}', '{page_title}', '{date}')")


if __name__ == '__main__':
    consumer = get_kafka_consumer()
    cassandra_client = get_cassandra_client()

    for msg in consumer:
        query1, query2 = compose_query(msg)
        print(f'[consumer]: new msg in queue: {msg}')
        cassandra_client.execute(query1)
        cassandra_client.execute(query2)
