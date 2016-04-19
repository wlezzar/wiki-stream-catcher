package net.lezzar.wikistream.output.sinks

import java.util.Properties

import net.lezzar.wikistream.clients.KafkaProducerFactory
import net.lezzar.wikistream.output.{RowOutputError, RowOutputSuccess, RowOutputStatus}
import org.apache.kafka.clients.producer.{ProducerRecord, Producer}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.{Failure, Success}

/**
  * Created by wlezzar on 08/03/16.
  */
class RddToKafkaSink[T](val name:String,
                         topic:String,
                         producer: => Producer[String, T])
  extends RddRowsInputBasedMonitorableSink[T] {

  def this(name:String, topic:String, conf:Properties) = this(
    name,
    topic,
    KafkaProducerFactory.create[String,T](conf)
  )

  // The actual row processing
  override def process(row: T): RowOutputStatus = {
    val producer = this.producer

    util.Try {
      producer.send(new ProducerRecord[String,T](topic, row)).get()
    } match {
      case Success(v) => RowOutputSuccess(s"topic : ${v.topic()} | partition : ${v.partition()} | offset : ${v.offset()}")
      case Failure(e) => RowOutputError(e.getMessage)
    }

  }
}
