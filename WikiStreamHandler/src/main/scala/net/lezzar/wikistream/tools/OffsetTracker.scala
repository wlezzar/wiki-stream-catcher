package net.lezzar.wikistream.tools

/**
  * Created by wlezzar on 21/02/16.
  */

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

import scala.collection.mutable.{Map => MutMap}
import net.lezzar.wikistream.tools.Utils

/* TODO : finish this */
class OffsetTracker(initalState:Map[(String, Int),Long]) extends Logging {

  private val internalState:MutMap[(String, Int),Long] = Utils.toMutMap(initalState)

  def updateState(topic:String, partition:Int, offset:Long) = internalState.put((topic,partition), offset)

  def getState():Map[(String, Int), Long] = internalState.toMap

  def getState(topic:String, partition:Int):Long = {
    internalState.getOrElse((topic, partition),{
      logError(s"Offset for topic '$topic' and partition '$partition' not found")
      0L
    })
  }
}

trait OffsetStore {
  def save(state:Map[(String, Int),Long]):Unit
  def restore():Option[Iterable[(String, Int,Long)]]
}

class CommiterOffsetTracker(offsetStore: OffsetStore, defaultState:Option[Iterable[(String, Int,Long)]] = None)
  extends OffsetTracker(
    (offsetStore.restore(), defaultState) match {
      case (Some(state),_)  => state
      case (None,Some(state))  => state
      case (None, None) =>
        throw new IllegalArgumentException("the given offset store is not able to retrieve " +
          "a valid state but no default state given")
    }
  ) {

  def commit(stopOnFailure:Boolean = false):Unit =
    try {
      offsetStore.save(getState())
    } catch {
      case e:Exception if stopOnFailure => throw e
      case e:Exception => logError(s"Failed to commit : ${e.getMessage}")
    }

}

class FileSystemOffsetStore(path:String, fsConf:Option[Configuration] = None) extends OffsetStore {

  private val fs:FileSystem = fsConf match {
    case Some(conf) => FileSystem.get(new URI(path), conf)
    case None => FileSystem.get(new URI(path), new Configuration())
  }

  override def save(state: Map[(String, Int), Long]): Unit = {
    val serialized = SerDe.serialize(state)
    val outputStream = fs.create(new Path(path), true)
    outputStream.write(serialized)
    outputStream.hsync()
    outputStream.close()
  }

  override def restore(): Option[Iterable[(String, Int, Long)]] = {
    try {
      val buffer = new Array[Byte](0)
      fs.open(new Path(path)).readFully(buffer)
      Some(SerDe.deserialize(buffer).asInstanceOf[Map[(String, Int), Long]])
    } catch {
      case e:Exception
    }


  }
}