#!/bin/sh

run_command() {
    docker exec "$1" /bin/sh -c "$2"
}

docker-compose up kafka cassandra mongo -d 

run_command kafka "kafka-topics.sh --create --topic wikipedia_events --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092"

run_command kafka "kafka-topics.sh --describe --topic wikipedia_events --bootstrap-server kafka:9092"

