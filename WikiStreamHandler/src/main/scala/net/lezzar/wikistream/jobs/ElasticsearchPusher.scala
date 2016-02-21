package net.lezzar.wikistream.jobs

import com.codahale.metrics.Gauge
import io.confluent.kafka.serializers.KafkaAvroDecoder
import net.lezzar.wikistream.SparkJob
import net.lezzar.wikistream.metrics.GlobalMetricRegistry
import net.lezzar.wikistream.output.OutputPipe
import net.lezzar.wikistream.output.sinks.JsonRddToEsSink
import net.lezzar.wikistream.tools.{OffsetTrackerMetricSource, OffsetTracker, Utils}
import org.apache.spark.streaming.StreamingContext
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
    KAFKA_TOPICS,
    ES_OUTPUT_MAPPING
  )

  //val currentOffsets = Utils.latestOffsets(get(KAFKA_CONF_BOOTSTRAP_SERVERS))
  //GlobalMetricRegistry.instance.register(s"${this.getClass.getSimpleName}.offsets")

  override def run(): Unit = {
    val kafkaConf = Map[String,String](
      "bootstrap.servers" -> get(KAFKA_CONF_BOOTSTRAP_SERVERS),
      "schema.registry.url" -> get(KAFKA_CONF_SCHEMA_REG_URL),
      "specific.avro.reader" -> "false"
    )

    val topic:String = get(KAFKA_TOPICS)

    val kafkaStream =
      KafkaUtils.createDirectStream[Object, Object,KafkaAvroDecoder, KafkaAvroDecoder] (ssc, kafkaConf, Set(topic))

    val sinks = List(
      new JsonRddToEsSink("EsSink", ES_OUTPUT_MAPPING, MyClients.esClient)
    )

    val outputPipe = new OutputPipe("WikiStreamPipe",sinks)

    /* for monitoring
    * 1 - Instantiate an offset tracker to track consumed messages. We need an
    * initial state for that
    * 2 - Instantiate an offset tracker metric source to watch the offset tracker state
    * 3 - Register the metric source to the global metric registry
    * */
    val initialOffsets = Utils
      .latestOffsets(get(KAFKA_CONF_BOOTSTRAP_SERVERS),topic)
      .map{case (p,o) => (topic, p, o)}

    val consumedOffsetsTracker = new OffsetTracker(initialOffsets)

    GlobalMetricRegistry.registerSource(
      new OffsetTrackerMetricSource("ElasticsearchPusher.consumed", consumedOffsetsTracker
    ))

    // Necessary to follow the offsets
    var offsetRanges = Array[OffsetRange]()

    kafkaStream
      .transform{rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
      .map(_._2.toString)
      .foreachRDD { rdd =>
        outputPipe(rdd)
        /* update offset tracker for monitoring */
        offsetRanges.foreach{ offRange =>
          consumedOffsetsTracker.updateState(
            offRange.topic,
            offRange.partition,
            offRange.untilOffset
          )}
        logInfo(s"info :\n${offsetRanges.mkString("\n")}")
      }

    ssc.start()
    ssc.awaitTermination()
  }
}

object ElasticsearchPusher {

  object Params {
    final val KAFKA_CONF_BOOTSTRAP_SERVERS = "kafka.conf.bootstrap.servers"
    final val KAFKA_CONF_SCHEMA_REG_URL = "kafka.conf.schema.registry.url"
    final val KAFKA_TOPICS = "kafka.topic"
    final val ES_OUTPUT_MAPPING = "elasticsearch.output.mapping"
  }

  def main(args: Array[String]) {
    import net.lezzar.wikistream.PrebuiltSparkContext._

    new ElasticsearchPusher(ssc, consArgsToMap(args)).run()
  }

}