package net.lezzar.wikistream.output.sinks

import net.lezzar.wikistream.output.{RowOutputError, RowOutputStatus, RowOutputSuccess}
import org.elasticsearch.client.Client

/**
  * Created by wlezzar on 20/02/16.
  */
class JsonRddToEsSink(val name:String, mapping:String, esClientByName: => Client)
  extends RddRowsInputBasedMonitorableSink[String] with Serializable {

  // The actual row processing
  override def process(row: String): RowOutputStatus = {
    val response = util.Try(esClientByName.prepareIndex("wiki_edits","raw_wiki_edits").setSource(row).get())
    response match {
      case util.Success(i) => RowOutputSuccess(s"document id : ${i.getId}")
      case util.Failure(e) => RowOutputError(e.getMessage)
    }
  }

}
