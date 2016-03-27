package net.lezzar.wikistream.output.sinks

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import net.lezzar.wikistream.clients.EsClient
import net.lezzar.wikistream.output.{RowOutputError, RowOutputStatus, RowOutputSuccess}
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient

import scala.util.Try

/**
  * Created by wlezzar on 20/02/16.
  */
class JsonRddToEsSink(val name:String, index:String, mapping:String, esClient: => Client)
  extends RddRowsInputBasedMonitorableSink[String] {

  def this(name:String,
           index:String,
           mapping:String,
           clusterName:String="elasticsearch",
           nodes:List[String]=List("localhost:9300")) =
    this(
      name,
      index,
      mapping,
      EsClient.getOrCreateSingleton(s"default-$clusterName-$nodes",clusterName, nodes).get
    )

  // The actual row processing
  override def process(row: String): RowOutputStatus = {
    val response = util.Try(
      esClient
        .prepareIndex(index, mapping)
        .setSource(row)
        .get()
    )

    response match {
      case util.Success(i) => RowOutputSuccess(s"document id : ${i.getId}")
      case util.Failure(e) => RowOutputError(e.getMessage)
    }
  }

}