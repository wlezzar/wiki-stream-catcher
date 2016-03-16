package net.lezzar.wikistream.jobs

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import net.lezzar.wikistream.tools._
import org.apache.kafka.clients.CommonClientConfigs._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}

import scala.util.{Failure, Success}

/**
  * Created by wlezzar on 10/03/16.
  */
abstract class KafkaDirectStreamJob[K, V, KD <: Decoder[K], VD <: Decoder[V]](ssc:StreamingContext,
                                                                              kafkaConfig: Map[String,String],
                                                                              topic:String,
                                                                              offsetStore:OffsetStore)(implicit evidence$19 : scala.reflect.ClassTag[K], evidence$20 : scala.reflect.ClassTag[V], evidence$21 : scala.reflect.ClassTag[KD], evidence$22 : scala.reflect.ClassTag[VD]) extends Logging {

  val offsetTracker = {
    val initialOffsets = offsetStore.restore().getOrElse(
      Utils.latestOffsets(kafkaConfig(BOOTSTRAP_SERVERS_CONFIG),topic) match {
        case Success(offsets) => offsets
          .map{case (partition,offset) => ((topic, partition), offset)}
          .toMap
        case Failure(e) => {
          logError(s"Unable to fetch offsets from ${kafkaConfig(BOOTSTRAP_SERVERS_CONFIG)}")
          throw e
        }}
    )
    new OffsetTracker(initialOffsets)
  }

  // Necessary to follow the offsets
  private var offsetRanges = Array[OffsetRange]()

  def start():Unit = {

    val kafkaStream = KafkaUtils.createDirectStream[K, V, KD, VD, MessageAndMetadata[K, V]](
      ssc,
      kafkaConfig,
      offsetTracker.state().map{case ((top, par),off) => (TopicAndPartition(top, par),off)},
      (v:MessageAndMetadata[K, V]) => v // for now, we only the message
    )

    val stream  = kafkaStream.transform{rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    process(stream)

    ssc.start()
    ssc.awaitTermination()
  }

  // WARNING : Must be used inside a stream so it can be used in every batch
  protected def commit():Unit = {
    /* update offset tracker state */
    offsetRanges.foreach{ offRange =>
      offsetTracker.updateState(
        offRange.topic,
        offRange.partition,
        offRange.untilOffset
      )}
    /* commit offsets */
    offsetStore.save(offsetTracker.state())
    logDebug(s"commited offsets :\n${offsetRanges.mkString("\n")}")
  }

  def process(stream:DStream[MessageAndMetadata[K,V]]):Unit
}
