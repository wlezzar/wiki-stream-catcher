# Start Zookeeper and expose port 2181 for use by the host machine
docker run -d --name zookeeper -p 2181:2181 confluent/zookeeper

# Start Kafka and expose port 9092 for use by the host machine
docker run -d --name kafka -p 9092:9092 --link zookeeper:zookeeper confluent/kafka

# Start Schema Registry and expose port 8081 for use by the host machine
docker run -d --name schema-registry -p 8081:8081 --link zookeeper:zookeeper --link kafka:kafka confluent/schema-registry

# Start REST Proxy and expose port 8082 for use by the host machine
docker run -d --name rest-proxy -p 8082:8082 --link zookeeper:zookeeper --link kafka:kafka --link schema-registry:schema-registry confluent/rest-proxy
    
# Start elasticsearch and export ports for use by the host machine
docker run -d --name elasticsearch elasticsearch

# Start elasticsearch and export ports for use by the host machine
docker run -d --name kibana --link elasticsearch:elasticsearch -p 5601:5601 kibana

