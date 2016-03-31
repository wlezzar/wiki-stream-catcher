package net.lezzar.wikistream.jobs.batch

import java.util.UUID

import io.confluent.kafka.serializers.KafkaAvroDecoder
import net.lezzar.wikistream.jobs._
import net.lezzar.wikistream.kafka.batch.KafkaBatchProcessor
import net.lezzar.wikistream.tools.{FileSystemOffsetStore, OffsetStore}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import java.net.URI

/**
  * Created by wlezzar on 28/03/16.
  */

class WikiEditsToFS(val sqlContext:SQLContext, val conf:Map[String,String]) extends KafkaBatchProcessor[Object, Object, KafkaAvroDecoder, KafkaAvroDecoder] with Job {

  import WikiEditsToFS.Params._

  override val optionalParams: Set[String] = Set()
  override val requiredParams: Set[String] = Set(
    KAFKA_CONF_BOOTSTRAP_SERVERS,
    KAFKA_CONF_SCHEMA_REG_URL,
    STREAM_SOURCE_TOPICS,
    OFFSET_STORE_PATH,
    BATCH_TARGET_BASEDIR
  )

  override val sc:SparkContext = sqlContext.sparkContext

  override def kafkaConfig: Map[String, String] = extractConf(List("kafka.clients.global."))

  override def offsetStore: OffsetStore = new FileSystemOffsetStore(get(OFFSET_STORE_PATH))

  override def topics: Set[String] = get[String](STREAM_SOURCE_TOPICS).split(",").toSet

  private val basedir = get[String](BATCH_TARGET_BASEDIR)

  private val fsInstance:FileSystem = FileSystem.get(new URI(basedir), new Configuration())

  override def process(batch: RDD[(String, Object)]): Unit = {
    val batchUUID = UUID.randomUUID().toString

    logInfo(s"Creating batch base folders...")
    topics.foreach(topic => fsInstance.mkdirs(new Path(s"$basedir/$topic")))

    val data = batch.map(x => (x._1, x._2.toString))
    data.persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val perTopicJsonRdd = topics.toList.map(topic => (topic, data.filter(_._1 == topic).map(_._2.toString)))
    perTopicJsonRdd.foreach { case (topic, rdd) =>
      val path = s"$basedir/$topic/$batchUUID"
      logInfo(s"storing batch data into parquet file : $path")
      sqlContext.read.json(rdd).repartition(8).write.parquet(path)
    }
  }

  override def run(): Unit = this.start()
}

object WikiEditsToFS {

  object Params {
    final val KAFKA_CONF_BOOTSTRAP_SERVERS = "kafka.clients.global.bootstrap.servers"
    final val KAFKA_CONF_SCHEMA_REG_URL = "kafka.clients.global.schema.registry.url"
    final val STREAM_SOURCE_TOPICS = "input.stream.kafka.topic"
    final val OFFSET_STORE_PATH = "input.stream.offset.store.path"
    final val BATCH_TARGET_BASEDIR = "batch.target.basedir"
  }

  def main(args: Array[String]) {
    import net.lezzar.wikistream.PrebuiltSparkContext._

    new WikiEditsToFS(sqlContext, consArgsToMap(args)).run()
  }

}
