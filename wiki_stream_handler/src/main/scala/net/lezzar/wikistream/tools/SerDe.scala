package net.lezzar.wikistream.tools

import java.io.{ByteArrayInputStream, ObjectInputStream, ObjectOutputStream, ByteArrayOutputStream}

/**
  * Created by wlezzar on 23/11/15.
  */
object SerDe {

  def serialize(obj:Object):Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(obj)
    oos.flush()
    baos.toByteArray
  }

  def deserialize(bytes:Array[Byte]) = new ObjectInputStream( new ByteArrayInputStream(bytes)).readObject()

}
