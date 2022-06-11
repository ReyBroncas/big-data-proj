import json
import argparse
from sseclient import SSEClient as EventSource
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import threading


def get_kafka_producer(bootstrap_server):
    try:
        producer = KafkaProducer(bootstrap_servers=bootstrap_server,
                                 value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    except NoBrokersAvailable:
        print('No broker found at {}'.format(bootstrap_server))
        raise

    if producer.bootstrap_connected():
        print('Kafka producer connected!')
        return producer
    else:
        print('Failed to establish connection!')
        exit(1)


def construct_event(data):
    user_types = {True: 'bot', False: 'human'}
    # use dictionary to change assign namespace value and catch any unknown namespaces (like ns 104)
    try:
        data['namespace'] = namespace_dict[data['namespace']]
    except KeyError:
        data['namespace'] = 'unknown'

    # assign user type value to either bot or human
    user_type = user_types[data['bot']]

    # define the structure of the json event that will be published to kafka topic
    event = {"id": data['id'],
             "domain": data['meta']['domain'],
             "namespace": data['namespace'],
             "title": data['title'],
             # "comment": event_data['comment'],
             "timestamp": data['meta']['dt'],  # event_data['timestamp'],
             "user_name": data['user'],
             "user_type": user_type,
             # "minor": event_data['minor'],
             "old_length": data['length']['old'],
             "new_length": data['length']['new']}

    return event


def init_namespaces():
    # create a dictionary for the various known namespaces
    # more info https://en.wikipedia.org/wiki/Wikipedia:Namespace#Programming
    namespace_dict = {-2: 'Media',
                      -1: 'Special',
                      0: 'main namespace',
                      1: 'Talk',
                      2: 'User', 3: 'User Talk',
                      4: 'Wikipedia', 5: 'Wikipedia Talk',
                      6: 'File', 7: 'File Talk',
                      8: 'MediaWiki', 9: 'MediaWiki Talk',
                      10: 'Template', 11: 'Template Talk',
                      12: 'Help', 13: 'Help Talk',
                      14: 'Category', 15: 'Category Talk',
                      100: 'Portal', 101: 'Portal Talk',
                      108: 'Book', 109: 'Book Talk',
                      118: 'Draft', 119: 'Draft Talk',
                      446: 'Education Program', 447: 'Education Program Talk',
                      710: 'TimedText', 711: 'TimedText Talk',
                      828: 'Module', 829: 'Module Talk',
                      2300: 'Gadget', 2301: 'Gadget Talk',
                      2302: 'Gadget definition', 2303: 'Gadget definition Talk'}

    return namespace_dict


def parse_command_line_arguments():
    parser = argparse.ArgumentParser(description='EventStreams Kafka producer')

    parser.add_argument('--bootstrap_server', default='localhost:9092', help='Kafka bootstrap broker(s) (host[:port])',
                        type=str)
    parser.add_argument('--topic_name', default='wikipedia-events', help='Destination topic name', type=str)
    parser.add_argument('--events_to_produce', help='Kill producer after n events have been produced', type=int,
                        default=1000)

    return parser.parse_args()


def save_data(data):
    time = data['rev_timestamp']
    id = data['rev_id']
    with open(f"data/{time}-{id}.json", "w") as write_file:
        json.dump(data, write_file, indent=4)

    print(f"saved new item: data/{time}-{id}.json")


if __name__ == "__main__":
    bootstrap_server = 'localhost:9092'
    topic_name = 'wikipedia_events'
    url = 'https://stream.wikimedia.org/v2/stream/page-create'
    events_num = 1000

    producer = get_kafka_producer(bootstrap_server)
    namespace_dict = init_namespaces()

    msg_count = 0
    for event in EventSource(url):

        if event.event == 'message':
            try:
                event_data = json.loads(event.data)
            except ValueError:
                pass
            else:
                # filter out events, keep only article edits (mediawiki.recentchange stream)
                # if event_data['type'] == 'edit':
                    # construct valid json event
                    # event_to_send = construct_event(event_data)

                    # save_data(event_data, time)
                    t1 = threading.Thread(target=save_data, args=[event_data])
                    t1.start()
                    # producer.send('wikipedia-events', value=event_to_send)
                    msg_count += 1

        if msg_count >= events_num:
            print('Producer will be killed as {} events were producted'.format(events_num))
            exit(0)
