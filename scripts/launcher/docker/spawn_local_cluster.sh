docker run -d --name zk01 \
  -e zk_id=1 \
  -e zk_server_1=zk01:2888:3888 \
  -e zk_server_2=zk02:2888:3888 \
  -e zk_server_3=zk03:2888:3888 \
  -e zk_maxClientCnxns=10 \
  cgswong/confluent-zookeeper

docker run -d --name zk02 \
  -e zk_id=2 \
  -e zk_server_1=zk01:2888:3888 \
  -e zk_server_2=zk02:2888:3888 \
  -e zk_server_3=zk03:2888:3888 \
  -e zk_maxClientCnxns=10 \
  cgswong/confluent-zookeeper

docker run -d --name zk03 \
  -e zk_id=3 \
  -e zk_server_1=zk01:2888:3888 \
  -e zk_server_2=zk02:2888:3888 \
  -e zk_server_3=zk03:2888:3888 \
  -e zk_maxClientCnxns=10 \
  cgswong/confluent-zookeeper

docker run -d --name kafka01 \
  -e kafka_broker_id=1 \
  -e kafka_zookeeper_connect=zk01:2181,zk02:2181,zk03:2181 \
  cgswong/confluent-kafka

docker run -d --name kafka02 \
  -e kafka_broker_id=2 \
  -e kafka_zookeeper_connect=zk01:2181,zk02:2181,zk03:2181 \
  cgswong/confluent-kafka

docker run -d --name kafka03 \
  -e kafka_broker_id=3 \
  -e kafka_zookeeper_connect=zk01:2181,zk02:2181,zk03:2181 \
  cgswong/confluent-kafka
