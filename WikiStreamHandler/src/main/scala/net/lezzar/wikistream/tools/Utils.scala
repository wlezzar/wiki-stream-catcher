package net.lezzar.wikistream.tools

import kafka.client.ClientUtils
import kafka.cluster.Broker
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import scala.collection.mutable.{Map => MutMap}
import scala.util.Try

/**
  * Created by wlezzar on 13/02/16.
  */
object Utils extends Logging {

  def latestOffsets(brokerList:String, topic : String):Try[Iterable[(Int,Long)]] = {
    import kafka.api.OffsetRequest._

    logInfo(s"Fetching latest offsets for topic $topic (brokers : $brokerList)")

    util.Try {
      val clientId = "latest-offset-getter"
      val brokers = ClientUtils.parseBrokerList(brokerList)
      val timeoutMs = 10000

      val topicsMetadata = ClientUtils.fetchTopicMetadata(Set(topic),brokers,clientId, timeoutMs).topicsMetadata

      if (topicsMetadata.size != 1 || !topicsMetadata.head.topic.equals(topic)) {
        System.err.println(("Error: no valid topic metadata for topic: %s, " + " probably the topic does not exist, run ").format(topic) +
          "kafka-list-topic.sh to verify")
        System.exit(1)
      }

      val partitionsMetadata = topicsMetadata.head.partitionsMetadata

      partitionsMetadata.map { partition =>
        partition.leader match {
          case Some(leader) => {
            val partitionId = partition.partitionId
            val consumer = new SimpleConsumer(leader.host, leader.port, timeoutMs, 10000, clientId)
            val lastOffset = consumer.earliestOrLatestOffset(TopicAndPartition(topic, partitionId), LatestTime, 1)
            (partitionId, lastOffset)
          }
          case None => throw new Exception(s"Partition ${partition.partitionId} for topic '$topic' does not have a leader !")
        }
      }
    }
  }

  def toMutMap(x:Iterable[(String, Int,Long)]):MutMap[(String, Int),Long] = {
    val y = MutMap[(String, Int),Long]()
    for ((a, b, c) <- x) y.put((a, b),c)
    y
  }

  def toMutMap(x:Map[(String, Int),Long]):MutMap[(String, Int),Long] = {
    val y = MutMap[(String, Int), Long]()
    for ((a,b) <- x) y.put((a._1,a._2),b)
    y
  }

}
