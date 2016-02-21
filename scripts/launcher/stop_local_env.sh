BASEDIR=$(dirname $0)
source $BASEDIR/env.sh

echo -e "\e[1;92mStopping elasticsearch...\e[0m"
ps ax | grep -i 'org\.elasticsearch\.bootstrap\.Elasticsearch' | grep java | grep -v grep | awk '{print $1}' | xargs kill -SIGTERM
sleep 3

echo -e "\e[1;92mStopping kibana...\e[0m"
ps ax | grep node | grep src | grep cli | grep -v grep | awk '{print $1}' | xargs kill -SIGTERM
sleep 3

echo -e "\e[1;92mStopping kafka rest...\e[0m"
$CONFLUENT_HOME/bin/kafka-rest-stop
sleep 5
echo -e "\e[1;92mStopping schema registry...\e[0m"
$CONFLUENT_HOME/bin/schema-registry-stop
sleep 5
echo -e "\e[1;92mStopping kafka broker...\e[0m"
$CONFLUENT_HOME/bin/kafka-server-stop
sleep 10
echo -e "\e[1;92mStopping Zookepper...\e[0m"
$CONFLUENT_HOME/bin/zookeeper-server-stop
sleep 15
echo -e "\e[1;92mDeleting zookeeper and kafka data...\e[0m"
rm -rf /tmp/zookeeper/ /tmp/kafka-logs/