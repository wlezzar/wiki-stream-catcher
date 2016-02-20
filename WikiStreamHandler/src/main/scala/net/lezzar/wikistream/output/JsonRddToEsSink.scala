package net.lezzar.wikistream.output

import com.codahale.metrics.MetricRegistry
import org.apache.spark.rdd.RDD
import org.elasticsearch.client.Client



class JsonRddToEsSink(esClientByName: => Client,
                      mapping:String,
                      val name:String,
                      metricsRegistryByName: => MetricRegistry) extends Sink[RDD[String]] with Serializable {

  override def apply(data: RDD[String]): SinkOutputStatus = {
    val (sucessCount, failsCount) = data
      .mapPartitions{ iter =>
        val outputCounts = iter.map{
          document =>
            val esClient = esClientByName
            // Metrics instatiation
            val metricsRegistry = metricsRegistryByName
            val totalMeter = metricsRegistry.meter(s"$name.messages.total")
            val succeededMeter = metricsRegistry.meter(s"$name.messages.success")
            val failedMeter = metricsRegistry.meter(s"$name.messages.success")
            // Sending the message
            totalMeter.mark()
            val response = util.Try(esClient.prepareIndex("wiki_edits","raw_wiki_edits").setSource(document).get())
            response match {
              case util.Success(i) => {
                succeededMeter.mark()
                (1,0)
              }
              case util.Failure(e) => {
                failedMeter.mark()
                (0,1)
              }
            }}
        val aggregated = if (outputCounts.isEmpty) (0,0) else outputCounts.reduce( (a,b) => (a._1+b._1, a._2+b._2) )
        Iterator(aggregated)}
      .reduce((a,b) => (a._1+b._1, a._2+b._2))
    SinkOutputStatus(sucessCount, failsCount)
  }
}