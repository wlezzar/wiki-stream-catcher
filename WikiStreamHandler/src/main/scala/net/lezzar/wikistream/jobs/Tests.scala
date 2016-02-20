package net.lezzar.wikistream.jobs

import java.net.InetAddress
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{ConsoleReporter, MetricRegistry}
import io.confluent.kafka.serializers.KafkaAvroDecoder
import kafka.common.TopicAndPartition
import net.lezzar.wikistream.output.{OutputPipe, JsonRddToEsSink}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.node.NodeBuilder

/**
  * Created by wlezzar on 25/01/16.
  */
object Tests {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WikiStream")
    val ssc = new StreamingContext(conf, Seconds(5))

    val brokers = "localhost:9092"
    val kafkaConf = Map[String,String](
      "bootstrap.servers"-> brokers,
      "schema.registry.url" -> "http://localhost:8081",
      "specific.avro.reader" -> "false")

    val topic = "WikiStreamEvents"

    val directKafka = KafkaUtils.createDirectStream[Object, Object,KafkaAvroDecoder, KafkaAvroDecoder](ssc, kafkaConf, Set(topic))
    directKafka
      .map(_._2.toString)
      .foreachRDD{ rdd =>
        val pipe = new OutputPipe("myPipe",List(new JsonRddToEsSink(MyClients.esClient, "wiki_edits/raw_wiki_edits","mySink",MyClients.metricReg)))
        val res = pipe(rdd)
        println(res)
      }

    //.foreachRDD(rdd => rdd.saveJsonToEs("wiki_edits/raw_wiki_edits", Map("es.mapping.id" -> "event_uuid")))

    ConsoleReporter
      .forRegistry(MyClients.metricReg)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build().start(5, TimeUnit.SECONDS)

    ssc.start()
    ssc.awaitTermination()

    MyClients.esClient.close()
  }
}

object MyClients {
  /*val esClient = NodeBuilder
    .nodeBuilder()
    .clusterName("lezzar-cluster")
    .settings(Settings.settingsBuilder().put("path.home","/opt/modules/elasticsearch/elasticsearch-2.2.0").build())
    .node().client()
    */
  val esClient = TransportClient
    .builder()
    .settings(Settings.settingsBuilder()
    .put("cluster.name", "lezzar-cluster").build())
    .build()
    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300))
  val metricReg = new MetricRegistry()
}
