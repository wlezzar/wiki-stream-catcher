package net.lezzar.wikistream.tools

import com.codahale.metrics.{Gauge, MetricRegistry}
import net.lezzar.wikistream.metrics.source.MetricSource

/**
  * Created by wlezzar on 21/02/16.
  */
class OffsetTrackerMetricSource(val sourceName:String, offsetTracker: OffsetTracker) extends MetricSource {

  override val metricRegistry: MetricRegistry = new MetricRegistry()

  offsetTracker.state.foreach{
    case ((topic,partition),_) => metricRegistry.register(s"$topic.$partition", new Gauge[Long] {
      override def getValue: Long = offsetTracker.state(topic,partition)
    })
  }

}
