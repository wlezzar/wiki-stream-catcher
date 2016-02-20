package net.lezzar.wikistream.output

/**
  * Created by wlezzar on 17/02/16.
  */
sealed abstract class RowOutputStatus extends Serializable with Product

case class RowOutputError(error:String) extends RowOutputStatus
case class RowOutputSuccess(success:String) extends RowOutputStatus