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

  override def run(): Unit = {
    val kafkaConf = Map[String,String](
      "bootstrap.servers" -> get(KAFKA_CONF_BOOTSTRAP_SERVERS),
      "schema.registry.url" -> get(KAFKA_CONF_SCHEMA_REG_URL),
      "specific.avro.reader" -> "false"
    )

    val topic:String = get(KAFKA_TOPICS)

    /** offset tracking and retrieving **/
    /************************************/

    /* 1 - Instantiate an offset store to restore last commited offsets and save future ones */
    val offsetStore = new FileSystemOffsetStore(get(OFFSET_STORE_PATH))

    /* 2 - Load the last commited state from the offset store or fetch the last ones from Kafka
     * in case they have not been retrieved */
    val initialOffsets = offsetStore.restore().getOrElse(
      Utils
        .latestOffsets(get(KAFKA_CONF_BOOTSTRAP_SERVERS),topic)
        .map{case (partition,offset) => ((topic, partition), offset)}
        .toMap
    )

    /* 3 - for offset tracking : Instantiate an offset tracker */
    val offsetTracker = new OffsetTracker(initialOffsets)

    /* 4 - for monitoring : Register an offset tracker metric source that reports the offset tracker state to
     * the global metric registry */
    GlobalMetricRegistry.registerSource(
      new OffsetTrackerMetricSource("ElasticsearchPusher.consumed", offsetTracker)
    )

    /** end of offset tracking preparation **/

    /* creating the main DStream */
    val kafkaStream = KafkaUtils.createDirectStream[Object, Object,KafkaAvroDecoder, KafkaAvroDecoder, Object](
      ssc,
      kafkaConf,
      initialOffsets.map{case ((top, par),off) => (TopicAndPartition(top, par),off)},
      (v:MessageAndMetadata[Object, Object]) => v.message() // for now, we only the message
    )

    val outputPipe = new OutputPipe(
      "WikiStreamPipe",
      sinks = List(new JsonRddToEsSink("EsSink", ES_OUTPUT_MAPPING, ElasticsearchClient.instance))
    )

    // Necessary to follow the offsets
    var offsetRanges = Array[OffsetRange]()

    kafkaStream
      // this transform step is a necessary boilerplate to access the consumed offsets
      .transform{rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
      // the real stream processing
      .map(_.toString)
      .foreachRDD { rdd =>
        outputPipe(rdd)
        /* update offset tracker state */
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
