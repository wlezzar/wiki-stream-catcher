package net.lezzar.wikistream.metrics.reporters

import java.util.concurrent.TimeUnit

import com.codahale.metrics.{ConsoleReporter, Reporter, MetricRegistry}

/**
  * Created by wlezzar on 20/02/16.
  */
class ConsoleMetricSink(registry:MetricRegistry, period:Int) extends MetricSink {
  val reporter: ConsoleReporter = ConsoleReporter
    .forRegistry(registry)
    .convertRatesTo(TimeUnit.SECONDS)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .build()

  override def start(): Unit = reporter.start(period, TimeUnit.SECONDS)

  override def stop(): Unit = reporter.stop()

  override def report(): Unit = reporter.report()
}
