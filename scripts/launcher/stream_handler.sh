BASENAME=$(dirname $0)
echo $BASENAME

source $BASENAME/env.sh

cd $PROJECT_HOME/WikiStreamHandler && mvn clean package && cd -

spark-submit --class net.lezzar.wikistream.jobs.ElasticsearchPusher --master local[*] $PROJECT_HOME/WikiStreamHandler/target/WikiStreamHandler-1.0-SNAPSHOT.jar "kafka.conf.bootstrap.servers=localhost:9092" "kafka.conf.schema.registry.url=http://localhost:8081" "kafka.topic=WikiStreamEvents" "elasticsearch.output.mapping=wiki_edits/raw_wiki_edits"
