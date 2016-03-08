package net.lezzar.wikistream.output.sinks

import net.lezzar.wikistream.output.{RowOutputError, RowOutputStatus, RowOutputSuccess}
import net.lezzar.wikistream.tools.ElasticsearchClient
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient

import scala.util.Try

/**
  * Created by wlezzar on 20/02/16.
  */
class JsonRddToEsSink(val name:String,
                      mapping:String,
                      esClient: => Client)
  extends RddRowsInputBasedMonitorableSink[String] with Serializable {

  def this(name:String, mapping:String,
           clusterName:String="elasticsearch",
           nodes:List[String]=List("localhost:9300")) =
    this(name, mapping, ElasticsearchClient.create(clusterName, nodes).get)

  // The actual row processing
  override def process(row: String): RowOutputStatus = {

    val response = util.Try(
      esClient
        .prepareIndex("wiki_edits","raw_wiki_edits")
        .setSource(row)
        .get()
    )

    response match {
      case util.Success(i) => RowOutputSuccess(s"document id : ${i.getId}")
      case util.Failure(e) => RowOutputError(e.getMessage)
    }
  }

}
