package net.lezzar.wikistream.jobs.streaming

import io.confluent.kafka.serializers.KafkaAvroDecoder
import kafka.message.MessageAndMetadata
import net.lezzar.wikistream.jobs._
import net.lezzar.wikistream.kafka.streaming.KafkaDirectStream
import net.lezzar.wikistream.output.OutputPipe
import net.lezzar.wikistream.output.sinks.JsonRddToEsSink
import net.lezzar.wikistream.tools._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by wlezzar on 20/02/16.
  */
class ElasticsearchPusher(val ssc:StreamingContext, val conf:Map[String,String])
  extends KafkaDirectStream[Object, Object, KafkaAvroDecoder, KafkaAvroDecoder]
    with Job {

  import ElasticsearchPusher.Params._

  override val optionalParams: Set[String] = Set()
  override val requiredParams: Set[String] = Set(
    KAFKA_CONF_BOOTSTRAP_SERVERS,
    KAFKA_CONF_SCHEMA_REG_URL,
    STREAM_SOURCE_TOPICS,
    ES_SERVER_HOSTS,
    ES_CLUSTER_NAME,
    ES_OUTPUT_MAPPING,
    OFFSET_STORE_PATH,
    KAFKA_SINK_TOPIC
  )

  override val name: String = "elasticsearch.pusher"

  override val kafkaConfig: Map[String, String] = this.extractConf(List("kafka.clients.global."))

  override val offsetStore: OffsetStore = new FileSystemOffsetStore(get(OFFSET_STORE_PATH))

  override val topics: Set[String] = get[String](STREAM_SOURCE_TOPICS).split(",").toSet

  override def process(stream: DStream[MessageAndMetadata[Object, Object]]): Unit = {
    val outputPipe = new OutputPipe(
      name  = "WikiStreamPipe",
      sinks = List(
        new JsonRddToEsSink(
          name        = "elasticsearch.sink",
          index       = get[String](ES_OUTPUT_MAPPING, _.split("/")(0)),
          mapping     = get[String](ES_OUTPUT_MAPPING, _.split("/")(1)),
          clusterName = get[String](ES_CLUSTER_NAME),
          nodes       = get[String](ES_SERVER_HOSTS).split(",").toList))
    )

    stream.map(_.message().toString).foreachRDD { rdd =>
      outputPipe(rdd)
      commit()
    }
  }

  override def run(): Unit = this.start()
}

object ElasticsearchPusher {

  object Params {
    final val KAFKA_CONF_BOOTSTRAP_SERVERS = "kafka.clients.global.bootstrap.servers"
    final val KAFKA_CONF_SCHEMA_REG_URL = "kafka.clients.global.schema.registry.url"
    final val STREAM_SOURCE_TOPICS = "input.stream.kafka.topic"
    final val ES_SERVER_HOSTS = "elasticsearch.server.hosts"
    final val ES_CLUSTER_NAME = "elasticsearch.cluster.name"
    final val ES_OUTPUT_MAPPING = "elasticsearch.output.mapping"
    final val OFFSET_STORE_PATH = "input.stream.offset.store.path"
    final val KAFKA_SINK_TOPIC = "archiver.target.topic"
  }

  def main(args: Array[String]) {
    import net.lezzar.wikistream.PrebuiltSparkContext._

    new ElasticsearchPusher(ssc, consArgsToMap(args)).run()
  }

}
