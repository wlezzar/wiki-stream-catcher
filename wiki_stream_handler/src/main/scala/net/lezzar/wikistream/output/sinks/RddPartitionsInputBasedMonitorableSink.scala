package net.lezzar.wikistream.output.sinks

import net.lezzar.wikistream.metrics.GlobalMetricRegistry
import net.lezzar.wikistream.output.{RowOutputError, RowOutputStatus, RowOutputSuccess}
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
  * Created by wlezzar on 20/02/16.
  */
abstract class RddPartitionsInputBasedMonitorableSink[T] extends Sink[RDD[T]] with Serializable {

  // The actual row processing
  def process(partition:Iterator[T]):Iterator[(T, RowOutputStatus)]

  // In case of failure, what to do with this row ? (must be overriden to handle errors)
  def fallback(row:T):RowOutputStatus = RowOutputSuccess("not handled")

  override def apply(data:RDD[T]):Unit = {
    // assign instance attributes to variables to enable using them without serializing the whole object
    val sinkName = this.name

    // Process rows, update metrics and log results
    data.foreachPartition{ iter =>
      val metricRegistry = GlobalMetricRegistry.instance
      val totalMeter = metricRegistry.meter(s"$sinkName.messages.total")
      val successMeter = metricRegistry.meter(s"$sinkName.messages.success")
      val failsMeter = metricRegistry.meter(s"$sinkName.messages.failures")

      val result = process(iter)

      result.foreach {
        case (_, RowOutputSuccess(msg)) => {
          Sink.logInfo(s"$sinkName - successfully sinked : $msg")
          totalMeter.mark()
          successMeter.mark()
        }
        case (row, RowOutputError(msg)) => {
          Sink.logError(s"$sinkName - failed to sink : $row ($msg)")
          totalMeter.mark()
          failsMeter.mark()
          fallback(row)
        }}
    }
  }
}
