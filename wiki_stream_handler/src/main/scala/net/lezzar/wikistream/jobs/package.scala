package net.lezzar.wikistream

/**
  * Created by wlezzar on 20/02/16.
  */
package object jobs {

  def consArgsToMap(args:Array[String]):Map[String,String] = {
    args
      .map(a => a.split("="))
      .map{
        case Array(key,value) => Map(key -> value)
        case Array(notValidArg) => throw new IllegalArgumentException(s"[$notValidArg] is not of the forme key=value")
      }
      .reduce(_ ++ _)
  }

}
