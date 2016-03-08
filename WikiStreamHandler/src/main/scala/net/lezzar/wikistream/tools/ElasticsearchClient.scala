package net.lezzar.wikistream.tools

import java.net.InetAddress

import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress

import scala.util.Try

/**
  * Created by wlezzar on 05/03/16.
  */
object ElasticsearchClient {
  def create(clusterName:String="elasticsearch", nodes:List[String]=List("localhost:9300")):Try[TransportClient] = {
    Try{
      val client = TransportClient
        .builder()
        .settings(Settings.settingsBuilder().put("cluster.name", clusterName).build())
        .build()
        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300))
      nodes.foldLeft(client){(c:TransportClient,node:String) =>
        require(node.matches("^\\S+:\\d+$"), s"elasticsearch url not valid : $node")
        val parts = node.split(":")
        val (host, port) = (parts(0),Integer.parseInt(parts(1)))
        c.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port))
      }
    }
  }

}
