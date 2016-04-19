package net.lezzar.wikistream.tools

/**
  * Created by wlezzar on 03/03/16.
  */
trait OffsetStore extends Logging {
  def save(state:Map[(String, Int),Long]):Unit
  def restore():Option[Map[(String, Int),Long]]
}
