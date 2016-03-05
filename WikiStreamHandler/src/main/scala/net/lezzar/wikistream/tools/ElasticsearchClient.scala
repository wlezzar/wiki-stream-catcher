package net.lezzar.wikistream.tools

import java.net.InetAddress

import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress

/**
  * Created by wlezzar on 05/03/16.
  */
object ElasticsearchClient {
  val instance = TransportClient
    .builder()
    .settings(Settings.settingsBuilder().put("cluster.name", "lezzar-cluster").build())
    .build()
    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300))
}
