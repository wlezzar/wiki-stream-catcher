package net.lezzar.wikistream.jobs.utils

import net.lezzar.wikistream.tools.Logging

/**
  * Created by wlezzar on 10/11/15.
  */

trait Configurable extends Logging {
  def conf:Map[String,String]

  logInfo(s"received configuration :\n${this.conf.mkString("\n")}")

  val requiredParams:Set[String]
  val optionalParams:Set[String]

  def get[T](key:String, thenApply:(String=>T) = (x:String) => x.asInstanceOf[T], default:Option[T]=None):T = {
    (this.conf.get(key), default) match {
      case (Some(element), _) => thenApply(element)
      case (None, Some(element)) => element
      case _ => throw new IllegalArgumentException(s"Key '$key' not found in the configuration map ($conf)")
    }
  }

  def assertValidConf() = {
    val givenParams = conf.keySet

    requiredParams -- givenParams match {
      case s if s.isEmpty => "ok"
      case s => throw new IllegalArgumentException(s"Some required parameters are missing : ${s.reduce(_ + ", "+ _)}")
    }
  }

  def extractConf(prefixes:List[String]):Map[String,String] = {
    val confs = prefixes.map { prefix =>
      this
        .conf
        .filterKeys(_ startsWith prefix)
        .map{ case (k,v) => (k.replaceFirst(prefix,""),v) }
    }
    if (confs.isEmpty) Map() else confs.reduce(_ ++ _)
  }
}
