package net.lezzar.wikistream.tools

/**
  * Created by wlezzar on 21/02/16.
  */

import kafka.api.OffsetRequest._

import scala.collection.mutable.{Map => MutMap}
import scala.util.{Failure, Success}

/* TODO : finish this */
class OffsetTracker(initalState:Map[(String, Int),Long]) extends Logging {

  private val internalState:MutMap[(String, Int),Long] = Utils.toMutMap(initalState)
  logInfo(s"offset tracker initialized with the following state : $internalState")

  def updateState(topic:String, partition:Int, offset:Long) = internalState.put((topic,partition), offset)

  def state():Map[(String, Int), Long] = internalState.toMap

  def state(topic:String, partition:Int):Long = internalState.getOrElse((topic, partition),{
      logError(s"Offset for topic '$topic' and partition '$partition' not found")
      0L
    })
}

object OffsetTracker extends Logging {

  private def fetchOffsets(brokers:String, topics:Set[String], time:Long) = Utils.latestOffsets(brokers,topics) match {
    case Success(offsets) => offsets
    case Failure(e) => {
      logError(s"Unable to fetch offsets from $brokers for topics $topics")
      throw e
    }}

  def apply(brokers:String, topics:Set[String]):OffsetTracker = {
    val latestOffsets = fetchOffsets(brokers, topics, LatestTime)
    new OffsetTracker(latestOffsets)
  }

  def apply(brokers:String, topics:Set[String], initialOffsets:Map[(String,Int),Long]) = {
    val earliestOffsets = fetchOffsets(brokers, topics, EarliestTime)
    val latestOffsets = fetchOffsets(brokers, topics, LatestTime)
    val mergedOffsets = latestOffsets.map{ x =>
      val (topicAndPartition, offset) = x
      (topicAndPartition,Math.min(offset, initialOffsets.getOrElse(topicAndPartition,earliestOffsets(topicAndPartition))))
    }
    new OffsetTracker(mergedOffsets)
  }
}