package net.lezzar.wikistream.output.sinks

/**
  * Created by wlezzar on 13/02/16.
  */

case class SinkOutputStatus(successCount:Int, failsCount:Int) {
  def registerSuccess = SinkOutputStatus(successCount+1, failsCount)
  def registerFailure = SinkOutputStatus(successCount, failsCount+1)
}
