package net.lezzar.wikistream.tools

import java.io.{EOFException, FileNotFoundException}
import java.net.URI

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}


/**
  * Created by wlezzar on 03/03/16.
  */
class FileSystemOffsetStore(path:String, fsConf:Configuration = new Configuration()) extends OffsetStore {

  private val fs:FileSystem = FileSystem.get(new URI(path), fsConf)

  override def save(state: Map[(String, Int), Long]): Unit = {
    val serialized = SerDe.serialize(state)
    val outputStream = fs.create(new Path(path), true)
    outputStream.write(serialized)
    outputStream.hsync()
    outputStream.close()
  }

  override def restore(): Option[Map[(String, Int), Long]] = {
    try {
      val data = IOUtils.toByteArray(fs.open(new Path(path)))
      val state = SerDe.deserialize(data).asInstanceOf[Map[(String, Int), Long]]
      logInfo(s"restored state : $state")
      Some(state)
    } catch {
      case e:FileNotFoundException => { logInfo(s"offset file not found at $path"); None}
      case e:EOFException => { logInfo(s"offset file found but is empty at $path"); None}
      case e:Throwable => { logError(s"unable to read offsets at $path"); throw e}
    }
  }
}

object FileSystemOffsetStore {
  def main(args: Array[String]) {
    //new FileSystemOffsetStore("/home/wlezzar/MyFiles/offsets").save(Map[(String,Int),Long](("topic",4)->5, ("hihi",8)->1))
    println(new FileSystemOffsetStore("/home/wlezzar/MyFiles/offsets").restore().get)
  }
}


