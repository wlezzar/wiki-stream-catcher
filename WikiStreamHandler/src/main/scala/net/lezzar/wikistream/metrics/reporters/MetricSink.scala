package net.lezzar.wikistream.metrics.reporters

import com.codahale.metrics.Reporter

/**
  * Created by wlezzar on 20/02/16.
  */
trait MetricSink {
  def start():Unit
  def stop():Unit
  def report():Unit
}
