BASENAME=$(dirname $0)
echo $BASENAME

source $BASENAME/env.sh

$SPARK_BIN/spark-submit --class net.lezzar.wikistream.jobs.ElasticsearchPusher \
  --master local[*] \
  $PROJECT_HOME/WikiStreamHandler/target/WikiStreamHandler-1.0-SNAPSHOT.jar \
  "kafka.clients.global.bootstrap.servers=localhost:9092" \
  "kafka.clients.global.schema.registry.url=http://localhost:8081" \
  "kafka.clients.global.specific.avro.reader=false" \
  "input.stream.kafka.topic=WikiStreamEvents" \
  "elasticsearch.cluster.name=lezzar-cluster" \
  "elasticsearch.server.hosts=localhost:9300" \
  "elasticsearch.output.mapping=wiki_edits/raw_wiki_edits" \
  "input.stream.offset.store.path=$PROJECT_HOME/scripts/launcher/run/offsets" \
  "archiver.target.topic=test_topic" \
  "kafka.clients.archiver.key.serializer=org.apache.kafka.common.serialization.StringSerializer" \
  "kafka.clients.archiver.value.serializer=org.apache.kafka.common.serialization.StringSerializer"
