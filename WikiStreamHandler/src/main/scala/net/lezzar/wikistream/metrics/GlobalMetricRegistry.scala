package net.lezzar.wikistream.metrics

import com.codahale.metrics.{ConsoleReporter, CsvReporter, JmxReporter, MetricRegistry}
import net.lezzar.wikistream.metrics.reporters.{ConsoleMetricSink, JmxMetricSink}
import net.lezzar.wikistream.metrics.source.MetricSource
import net.lezzar.wikistream.tools.Logging

/**
  * Created by wlezzar on 20/02/16.
  */
object GlobalMetricRegistry extends Logging {

  val instance: MetricRegistry = {
    val registry = new MetricRegistry()
    List("jmx").flatMap(sink => registerSink(sink, registry)).foreach(_.start())
    registry
  }

  def registerSink(sink:String, registry : MetricRegistry) = sink match {
    case "jmx" => Some(new JmxMetricSink(registry))
    case "console" => Some(new ConsoleMetricSink(registry,5))
    case _ => None
  }

  def registerSource(source:MetricSource):Unit = {
    logInfo(s"registering source ${source.sourceName}")
    instance.register(source.sourceName, source.metricRegistry)
  }

}
