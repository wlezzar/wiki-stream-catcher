package net.lezzar.wikistream.output.sinks

import net.lezzar.wikistream.output.RowOutputStatus

/**
  * Created by wlezzar on 20/02/16.
  */
abstract class RddRowsInputBasedMonitorableSink[T] extends RddPartitionsInputBasedMonitorableSink[T] {

  // The actual row processing
  def process(row:T):RowOutputStatus

  override def process(partition:Iterator[T]):Iterator[(T, RowOutputStatus)] = {
    partition.map(row => (row, process(row)))
  }
}
