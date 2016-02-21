package net.lezzar.wikistream.tools

/**
  * Created by wlezzar on 21/02/16.
  */

import scala.collection.mutable.{Map => MutMap}

class OffsetTracker(initalState:Iterable[(String, Int,Long)]) extends Logging {

  private val internalState:MutMap[(String, Int),Long] = {
    val state = MutMap[(String, Int),Long]()
    for ((topic, partition, offset) <- initalState) state.put((topic, partition),offset)
    state
  }

  def updateState(topic:String, partition:Int, offset:Long) = internalState.put((topic,partition), offset)

  def getState():Map[(String, Int), Long] = internalState.toMap

  def getState(topic:String, partition:Int):Long = {
    internalState.getOrElse((topic, partition),{
      logError(s"Offset for topic '$topic' and partition '$partition' not found")
      0L
    })
  }
}
