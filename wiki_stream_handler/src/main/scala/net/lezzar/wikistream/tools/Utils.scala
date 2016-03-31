package net.lezzar.wikistream.tools

import kafka.api.OffsetRequest._
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

  def fetchLeaders(brokerList:String, topics:Set[String]):Map[(String,Int), Option[Broker]] = {
    val clientId = "leaders-getter"
    val brokers = ClientUtils.parseBrokerList(brokerList)
    val timeoutMs = 10000

    val topicsMetadata = ClientUtils.fetchTopicMetadata(topics,brokers,clientId, timeoutMs).topicsMetadata

    topicsMetadata.flatMap{ topicMetadata =>
      val topic = topicMetadata.topic
      topicMetadata.partitionsMetadata.map(x => ((topic, x.partitionId), x.leader))
    }.toMap
  }

  def offsetRequest(brokerList:String, topics : Set[String], request:Long):Try[Map[(String, Int), Long]] = {
    logInfo(s"Fetching latest offsets for topics $topics (brokers : $brokerList)")

    util.Try {
      val clientId = "latest-offset-getter"
      val brokers = ClientUtils.parseBrokerList(brokerList)
      val timeoutMs = 10000

      val topicsMetadata = ClientUtils.fetchTopicMetadata(topics,brokers,clientId, timeoutMs).topicsMetadata

      if (topicsMetadata.size != 1 || !topicsMetadata.map(_.topic).toSet.equals(topics)) {
        System.err.println(("Error: no valid topics metadata for topics: %s, " + " probably the topic does not exist, run ").format(topics) +
          "kafka-list-topic.sh to verify")
        System.exit(1)
      }

      topicsMetadata.flatMap{ topicMetadata =>
        val topic = topicMetadata.topic
        val partitionsMetadata = topicMetadata.partitionsMetadata
        partitionsMetadata.map { partition =>
          partition.leader match {
            case Some(leader) => {
              val partitionId = partition.partitionId
              val consumer = new SimpleConsumer(leader.host, leader.port, timeoutMs, 10000, clientId)
              val lastOffset = consumer.earliestOrLatestOffset(TopicAndPartition(topic, partitionId), request, 1)
              ((topic, partitionId), lastOffset)
            }
            case None => throw new Exception(s"Partition ${partition.partitionId} for topic '$topic' does not have a leader !")
          }
        }
      }.toMap
    }
  }

  def latestOffsets(brokerList:String, topics : Set[String]):Try[Map[(String, Int), Long]] = {
    offsetRequest(brokerList, topics, LatestTime)
  }

  def earliestOffsets(brokerList:String, topics : Set[String]):Try[Map[(String, Int), Long]] = {
    offsetRequest(brokerList, topics, EarliestTime)
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
