package net.lezzar.wikistream.jobs

import io.confluent.kafka.serializers.KafkaAvroDecoder
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import net.lezzar.wikistream.SparkJob
import net.lezzar.wikistream.metrics.GlobalMetricRegistry
import net.lezzar.wikistream.output.OutputPipe
import net.lezzar.wikistream.output.sinks.JsonRddToEsSink
import net.lezzar.wikistream.tools._
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
    ES_OUTPUT_MAPPING,
    OFFSET_STORE_PATH
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

    /* offset tracking :
    * 1 - Instantiate an offset store to restore last commited offsets and save future ones
    * 2 - Load the last commited state or fetch the last ones from Kafka in case they have not
    * been stored
    * 3 - for monitoring : Instantiate an offset tracker metric source to report the offset
    * tracker state
    * 4 - Register the metric source to the global metric registry
    * */
    val offsetStore = new FileSystemOffsetStore(get(OFFSET_STORE_PATH))

    val initialOffsets = offsetStore.restore().getOrElse(
      Utils
        .latestOffsets(get(KAFKA_CONF_BOOTSTRAP_SERVERS),topic)
        .map{case (partition,offset) => ((topic, partition), offset)}
        .toMap
    )

    val offsetTracker = new OffsetTracker(initialOffsets)

    GlobalMetricRegistry.registerSource(
      new OffsetTrackerMetricSource("ElasticsearchPusher.consumed", offsetTracker
      ))

    /* creating the main DStream */
    val kafkaStream = KafkaUtils.createDirectStream[Object, Object,KafkaAvroDecoder, KafkaAvroDecoder, Object](
      ssc,
      kafkaConf,
      initialOffsets.map{case ((top, par),off) => (TopicAndPartition(top, par),off)},
      (v:MessageAndMetadata[Object, Object]) => v.message()
    )

    val sinks = List(
      new JsonRddToEsSink("EsSink", ES_OUTPUT_MAPPING, ElasticsearchClient.instance)
    )

    val outputPipe = new OutputPipe("WikiStreamPipe",sinks)

    // Necessary to follow the offsets
    var offsetRanges = Array[OffsetRange]()

    kafkaStream
      .transform{rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
      .map(_.toString)
      .foreachRDD { rdd =>
        outputPipe(rdd)
        /* update offset tracker for monitoring */
        offsetRanges.foreach{ offRange =>
          offsetTracker.updateState(
            offRange.topic,
            offRange.partition,
            offRange.untilOffset
          )}
        /* commit offsets */
        offsetStore.save(offsetTracker.getState())
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
    final val OFFSET_STORE_PATH = "offset.store.path"
  }

  def main(args: Array[String]) {
    import net.lezzar.wikistream.PrebuiltSparkContext._

    new ElasticsearchPusher(ssc, consArgsToMap(args)).run()
  }

}
