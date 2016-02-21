package net.lezzar.wikistream.metrics.reporters

import com.codahale.metrics.{MetricRegistry, JmxReporter, Reporter}

/**
  * Created by wlezzar on 20/02/16.
  */
class JmxMetricSink(registry:MetricRegistry) extends MetricSink {
  val reporter: JmxReporter = JmxReporter.forRegistry(registry).build()

  override def start(): Unit = reporter.start()

  override def stop(): Unit = reporter.stop()

  override def report(): Unit = Unit
}
