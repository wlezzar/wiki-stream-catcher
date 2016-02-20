package net.lezzar.wikistream.tools

import kafka.client.ClientUtils
import kafka.cluster.Broker
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer

/**
  * Created by wlezzar on 13/02/16.
  */
object Utils {

  def latestOffsets(brokerList:String, topic : String):Iterable[(Int,Long)] = {
    import kafka.api.OffsetRequest._

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
