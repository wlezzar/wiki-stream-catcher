package net.lezzar.wikistream.output.sinks

import net.lezzar.wikistream.tools.Logging

/**
  * Created by wlezzar on 17/02/16.
  */
trait Sink[T] {
  def name:String
  def apply(data:T):Unit
}

object Sink extends Logging
