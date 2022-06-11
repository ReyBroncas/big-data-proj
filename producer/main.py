import json
from sseclient import SSEClient as EventSource
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from datetime import timedelta
from dateutil import parser
import logging

WIKIPEDIA_DATA_STREAM = 'https://stream.wikimedia.org/v2/stream/page-create'
KAFKA_TOPIC = 'wikipedia_events'
KAFKA_BOOTSTRAP_SERVER = 'kafka:9092'

logging.basicConfig(level=logging.DEBUG)


def get_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
    except NoBrokersAvailable:
        print('No broker found at {}'.format(KAFKA_BOOTSTRAP_SERVER))
        raise

    if producer.bootstrap_connected():
        print('Kafka producer connected!')
        return producer
    else:
        print('Failed to establish connection!')
        exit(1)


def compose_msg(raw_data, timestamp):
    if not raw_data['performer'].get('user_id'):
        return

    return {
        'domain': raw_data['meta']['domain'],
        'is_bot': raw_data['performer']['user_is_bot'],
        'page_title': raw_data['page_title'],
        'user_id': raw_data['performer']['user_id'],
        'user_name': raw_data['performer']['user_text'],
        'timestamp': str(timestamp),
    }


def produce():
    producer = get_kafka_producer()

    for event in EventSource(WIKIPEDIA_DATA_STREAM):
        if event.event == 'message':
            try:
                event_data = json.loads(event.data)
            except ValueError:
                pass
            else:
                timestamp = parser.parse(event_data['rev_timestamp']) + timedelta(hours=3)
                msg = compose_msg(event_data, timestamp)

                if msg:
                    producer.send(KAFKA_TOPIC, value=msg)
                    print('[producer]: added new item')


if __name__ == '__main__':
    produce()
