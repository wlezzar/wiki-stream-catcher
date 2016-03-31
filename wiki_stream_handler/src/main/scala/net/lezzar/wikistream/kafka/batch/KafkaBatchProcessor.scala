package net.lezzar.wikistream.kafka.batch

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import net.lezzar.wikistream.tools.{Utils, OffsetTracker, OffsetStore, Logging}
import org.apache.kafka.clients.CommonClientConfigs._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.{Broker, OffsetRange, KafkaUtils}

/**
  * Created by wlezzar on 29/03/16.
  */
abstract class KafkaBatchProcessor [K, V, KD <: Decoder[K], VD <: Decoder[V]](implicit evidence$19 : scala.reflect.ClassTag[K], evidence$20 : scala.reflect.ClassTag[V], evidence$21 : scala.reflect.ClassTag[KD], evidence$22 : scala.reflect.ClassTag[VD]) extends Logging {

  def sc:SparkContext
  def kafkaConfig: Map[String,String]
  def offsetStore:OffsetStore
  def topics:Set[String]

  lazy val offsetTracker = offsetStore.restore() match {
    case None => OffsetTracker(kafkaConfig(BOOTSTRAP_SERVERS_CONFIG), topics)
    case Some(storedOffsets) => OffsetTracker(kafkaConfig(BOOTSTRAP_SERVERS_CONFIG), topics, storedOffsets)
  }

  def process(batch:RDD[(String,V)])

  def start() = {

    val brokers = kafkaConfig(BOOTSTRAP_SERVERS_CONFIG)
    val leaders = Utils.fetchLeaders(brokers, topics).map{
      case ((topic,partition),Some(broker)) => (TopicAndPartition(topic,partition),Broker(broker.host, broker.port))
      case ((topic,partition),None) => throw new Exception(s"partition $partition of topic $topic does not have a leader")
    }

    val toOffsets = Utils.latestOffsets(brokers,topics).get

    val offsetRanges = offsetTracker
      .state()
      .map{ case ((topic,part),fromOffset) => OffsetRange(topic,part,fromOffset,toOffsets((topic,part)))}
      .toArray

    logInfo(s"Batch RDD offsets :\n${offsetRanges.mkString("\n")}")

    val batch = KafkaUtils.createRDD[K,V,KD,VD,(String,V)](
      sc,
      kafkaConfig,
      offsetRanges,
      leaders,
      (v:MessageAndMetadata[K, V]) => (v.topic, v.message)
    )

    logInfo("Processing batch...")

    process(batch)

    offsetStore.save(toOffsets)
  }
}
