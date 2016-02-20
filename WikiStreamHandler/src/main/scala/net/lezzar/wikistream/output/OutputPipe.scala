package net.lezzar.wikistream.output

/**
  * Created by wlezzar on 17/02/16.
  */
class OutputPipe[T](name:String, sinks:Iterable[Sink[T]]) {
  def apply(data:T):Iterable[SinkOutputStatus] = sinks.map(sink => sink(data))
}
