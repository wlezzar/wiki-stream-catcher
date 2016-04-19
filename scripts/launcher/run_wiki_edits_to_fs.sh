BASENAME=$(dirname $0)
echo $BASENAME

source $BASENAME/env.sh

$SPARK_BIN/spark-submit --class net.lezzar.wikistream.jobs.batch.WikiEditsToFS \
  --master local[*] \
  $PROJECT_HOME/wiki_stream_handler/target/WikiStreamHandler-1.0-SNAPSHOT.jar \
  "kafka.clients.global.bootstrap.servers=localhost:9092" \
  "kafka.clients.global.schema.registry.url=http://localhost:8081" \
  "kafka.clients.global.specific.avro.reader=false" \
  "input.stream.kafka.topic=WikiStreamEvents" \
  "input.stream.offset.store.path=$PROJECT_HOME/scripts/launcher/run/batches/offsets" \
  "batch.target.basedir=$PROJECT_HOME/scripts/launcher/run/batches"
