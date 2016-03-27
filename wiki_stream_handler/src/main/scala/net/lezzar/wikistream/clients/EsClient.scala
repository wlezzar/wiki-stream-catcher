package net.lezzar.wikistream.clients

import java.net.InetAddress

import net.lezzar.wikistream.tools.Logging
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress

import scala.collection.mutable.{Map=>MutMap}

import scala.util.{Success, Try}

/**
  * Created by wlezzar on 05/03/16.
  */
object EsClient extends Logging {

  val singletons:MutMap[String, Client] = MutMap()

  def create(clusterName:String="elasticsearch", nodes:List[String]=List("localhost:9300")):Try[TransportClient] = {
    Try{
      logInfo(s"Creating an ES client (cluster : $clusterName, nodes : $nodes")
      val client = TransportClient
        .builder()
        .settings(Settings.settingsBuilder().put("cluster.name", clusterName).build())
        .build()
      nodes.foldLeft(client){(c:TransportClient,node:String) =>
        require(node.matches("^\\S+:\\d+$"), s"elasticsearch url not valid : $node")
        val parts = node.split(":")
        val (host, port) = (parts(0),Integer.parseInt(parts(1)))
        c.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port))
      }
    }
  }

  def getOrCreateSingleton(id:String, clusterName:String, nodes:List[String]=List("localhost:9300")):Try[Client] = {
    util.Try(singletons.getOrElseUpdate(id, EsClient.create(clusterName, nodes).get))
  }
}
