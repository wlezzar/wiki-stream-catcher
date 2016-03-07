docker run --rm --name controller -it cgswong/confluent-platform

docker exec -it zk1 bin/bash

docker attach --sig-proxy=false kafka01

# Topics management
kafka-topics --create --topic test_topic --zookeeper zk01:2181,zk02:2181,zk03:2181 --replication-factor 3 --partition 12
kafka-topics --describe --topic test_topic --zookeeper zk01:2181,zk02:2181,zk03:2181

# Produce and consume messages
kafka-console-producer --topic test_topic --broker-list kafka01:9092,kafka02:9092,kafka03:9092
kafka-console-consumer --new-consumer --topic test_topic --bootstrap-server kafka03:9092,kafka01:9092,kafka02:9092

# Groups management
kafka-consumer-groups --list --new-consumer --bootstrap-server kafka01:9092,kafka02:9092,kafka03:9092
kafka-consumer-groups --describe --group console-consumer-67690 --new-consumer --bootstrap-server kafka01:9092,kafka02:9092,kafka03:9092
