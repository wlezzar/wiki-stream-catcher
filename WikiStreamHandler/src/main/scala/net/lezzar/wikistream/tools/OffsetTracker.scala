package net.lezzar.wikistream.tools

/**
  * Created by wlezzar on 21/02/16.
  */

import scala.collection.mutable.{Map => MutMap}

/* TODO : finish this */
class OffsetTracker(initalState:Map[(String, Int),Long]) extends Logging {

  private val internalState:MutMap[(String, Int),Long] = Utils.toMutMap(initalState)
  logInfo(s"offset tracker initialized with the following state : $internalState")

  def updateState(topic:String, partition:Int, offset:Long) = internalState.put((topic,partition), offset)

  def getState():Map[(String, Int), Long] = internalState.toMap

  def getState(topic:String, partition:Int):Long = internalState.getOrElse((topic, partition),{
      logError(s"Offset for topic '$topic' and partition '$partition' not found")
      0L
    })
}