package net.lezzar.wikistream.output

import net.lezzar.wikistream.output.sinks.Sink
import net.lezzar.wikistream.tools.Logging

/**
  * Created by wlezzar on 17/02/16.
  */
class OutputPipe[T](name:String, sinks:Iterable[Sink[T]]) {
  def apply(data:T):Unit = sinks.foreach(sink => sink(data))
}
