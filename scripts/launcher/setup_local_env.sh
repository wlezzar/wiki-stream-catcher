BASEDIR=$(dirname $0)
source $BASEDIR/env.sh

$BASEDIR/stop_local_env.sh

LOGS_DIR=$PROJECT_HOME/scripts/launcher/logs

echo -e "\e[1;92mStarting Zookepper...\e[0m"
$CONFLUENT_HOME/bin/zookeeper-server-start $CONFLUENT_HOME/etc/kafka/zookeeper.properties > $LOGS_DIR/zookeeper.log 2>&1 &
sleep 5
echo -e "\e[1;92mStarting kafka broker...\e[0m"
$CONFLUENT_HOME/bin/kafka-server-start $CONFLUENT_HOME/etc/kafka/server.properties > $LOGS_DIR/kafka.log 2>&1 &
sleep 10
echo -e "\e[1;92mStarting schema registry...\e[0m"
$CONFLUENT_HOME/bin/schema-registry-start $CONFLUENT_HOME/etc/schema-registry/schema-registry.properties > $LOGS_DIR/schema_registry.log 2>&1 &
sleep 10
echo -e "\e[1;92mStarting kafka rest...\e[0m"
$CONFLUENT_HOME/bin/kafka-rest-start $CONFLUENT_HOME/etc/kafka-rest/kafka-rest.properties > $LOGS_DIR/kafka_rest.log 2>&1 &
sleep 5

echo -e "\e[1;92mStarting elasticsearch...\e[0m"
$ELASTIC_HOME/bin/elasticsearch > $LOGS_DIR/elasticsearch.log 2>&1 &
sleep 10

echo -e "\e[1;92mStarting kibana...\e[0m"
$KIBANA_HOME/bin/kibana > $LOGS_DIR/kibana.log 2>&1 &
sleep 5

echo -e "\e[1;92mCreating schemas in schema registry...\e[0m"
$PROJECT_HOME/models/schemas/confluent/create_schema_on_schema_registry.sh
sleep 3

echo -e "\e[1;92mCreating kafka topics...\e[0m"
$PROJECT_HOME/models/schemas/kafka/create_topics.sh
sleep 3

echo -e "\e[1;92mCreating elasticsearch mappings...\e[0m"
$PROJECT_HOME/models/schemas/elasticsearch/create_index.sh