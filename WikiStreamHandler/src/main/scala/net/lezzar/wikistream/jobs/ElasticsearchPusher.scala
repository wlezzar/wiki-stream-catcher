package net.lezzar.wikistream.jobs

import java.util.Properties

import io.confluent.kafka.serializers.KafkaAvroDecoder
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import net.lezzar.wikistream.SparkJob
import net.lezzar.wikistream.metrics.GlobalMetricRegistry
import net.lezzar.wikistream.output.OutputPipe
import net.lezzar.wikistream.output.sinks.JsonRddToEsSink
import net.lezzar.wikistream.tools._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange, KafkaUtils}

/**
  * Created by wlezzar on 20/02/16.
  */
class ElasticsearchPusher(_ssc:StreamingContext, conf:Map[String,String]) extends SparkJob(_ssc, conf) {

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

  override def run(): Unit = {

    val pusher = new KafkaDirectStreamJob[Object, Object, KafkaAvroDecoder, KafkaAvroDecoder](
      ssc,
      extractConf(List("kafka.clients.global.")),
      get(STREAM_SOURCE_TOPICS),
      new FileSystemOffsetStore(get(OFFSET_STORE_PATH))) {

      val outputPipe = new OutputPipe(
        "WikiStreamPipe",
        sinks = List(
          new JsonRddToEsSink(
            name = "elasticsearch",
            index = get(ES_OUTPUT_MAPPING, _.split("/")(0)),
            mapping = get(ES_OUTPUT_MAPPING, _.split("/")(1)),
            clusterName = get(ES_CLUSTER_NAME),
            nodes = get[String](ES_SERVER_HOSTS).split(",").toList))
      )

      override def process(stream: DStream[MessageAndMetadata[Object, Object]]): Unit = {
        stream.map(_.message().toString).foreachRDD { rdd =>
          outputPipe(rdd)
          commit()
        }
      }

    }

    GlobalMetricRegistry.registerSource(
      new OffsetTrackerMetricSource("elasticsearch.pusher.offsets",pusher.offsetTracker)
    )

    pusher.start()
  }

  def extractConf(prefixes:List[String]):Map[String,String] = {
    val confs = prefixes.map { prefix =>
      this
        .conf
        .filterKeys(_ startsWith prefix)
        .map{ case (k,v) => (k.replaceFirst(prefix,""),v) }
    }
    if (confs.isEmpty) Map() else confs.reduce(_ ++ _)
  }
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
