package net.lezzar.wikistream.metrics.source

import com.codahale.metrics.MetricRegistry

/**
  * Created by wlezzar on 21/02/16.
  */
trait MetricSource {
  def sourceName:String
  def metricRegistry:MetricRegistry
}
