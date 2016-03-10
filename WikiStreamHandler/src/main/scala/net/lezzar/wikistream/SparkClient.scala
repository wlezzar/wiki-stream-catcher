package net.lezzar.wikistream

import net.lezzar.wikistream.tools.Logging
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wlezzar on 10/11/15.
  */

abstract class SparkClient(protected val sc:SparkContext = null,
                           protected val sqlContext:SQLContext = null,
                           protected val ssc:StreamingContext = null) extends Logging {
  def this(_sqlContext: SQLContext, _ssc: StreamingContext = null) = this(_sqlContext.sparkContext, _sqlContext, _ssc)
  def this(_ssc: StreamingContext) = this(_ssc.sparkContext, null, _ssc)
}

trait Configurable {
  def conf:Map[String,String]

  val requiredParams:Set[String]
  val optionalParams:Set[String]

  protected def get[T](key:String, thenApply:(String=>T) = (x:String) => x.asInstanceOf[T],default:T=null):T = {
    if (default != null || conf.contains(key)) thenApply(conf.getOrElse(key,default.toString))
    else throw new IllegalArgumentException(s"Key '$key' not found in the configuration map ($conf)")
  }

  protected def assertValidConf() = {
    val givenParams = conf.keySet

    requiredParams -- givenParams match {
      case s if s.isEmpty => "ok"
      case s => throw new IllegalArgumentException(s"Some required parameters are missing : ${s.reduce(_ + ", "+ _)}")
    }
  }
}

abstract class SparkJob(_sc:SparkContext, _sqlContext:SQLContext = null, _ssc:StreamingContext = null, val conf:Map[String,String] = Map()) extends SparkClient(_sc,_sqlContext,_ssc) with Configurable {

  def this(_sqlContext: SQLContext, _ssc: StreamingContext = null, _conf:Map[String,String] = Map()) = this(_sqlContext.sparkContext, _sqlContext, _ssc, _conf)
  def this(_sqlContext: SQLContext, _conf:Map[String,String]) = this(_sqlContext.sparkContext, _sqlContext, null, _conf)
  def this(_ssc: StreamingContext, _conf:Map[String,String] = Map()) = this(_ssc.sparkContext, null, _ssc, _conf)

  logInfo(s"received configuration :\n${this.conf.mkString("\n")}")

  def run():Unit
}


object PrebuiltSparkContext extends Logging {

  val conf = new SparkConf()

  lazy val sc: SparkContext = {

    try {
      conf.get("spark.master")
    } catch {
      case e: NoSuchElementException =>
        logWarn("No master is set, using 'local[*]'")
        conf.setMaster("local[*]")
    }

    val _sc = new SparkContext(conf.setAppName(this.getClass.getName))

    val s3AccessKey: String = System.getenv("AWS_ACCESS_KEY_ID")
    val s3SecretKey: String = System.getenv("AWS_SECRET_ACCESS_KEY")
    if (s3AccessKey != null && s3SecretKey != null) {
      _sc.hadoopConfiguration.set("fs.s3a.access.key", s3AccessKey)
      _sc.hadoopConfiguration.set("fs.s3a.secret.key", s3SecretKey)
      _sc.hadoopConfiguration.set("fs.s3n.access.key", s3AccessKey)
      _sc.hadoopConfiguration.set("fs.s3n.secret.key", s3SecretKey)
      SparkHadoopUtil.get.conf.set("fs.s3a.access.key", System.getenv("AWS_ACCESS_KEY_ID"))
      SparkHadoopUtil.get.conf.set("fs.s3a.secret.key", System.getenv("AWS_SECRET_ACCESS_KEY"))
    }
    _sc
  }

  lazy val sqlContext: SQLContext = {

    val _sqlContext = new SQLContext(sc)
    //_sqlContext.setConf("spark.sql.shuffle.partitions", get("nbPartitions"))
    _sqlContext.setConf("park.sql.parquet.mergeSchema", "true")
    val s3AccessKey: String = System.getenv("AWS_ACCESS_KEY_ID")
    val s3SecretKey: String = System.getenv("AWS_SECRET_ACCESS_KEY")
    if (s3AccessKey != null && s3SecretKey != null) {
      _sqlContext.setConf("fs.s3a.access.key", s3AccessKey)
      _sqlContext.setConf("fs.s3a.secret.key", s3SecretKey)
      _sqlContext.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", s3AccessKey)
      _sqlContext.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", s3SecretKey)
      _sqlContext.sparkContext.hadoopConfiguration.set("fs.s3n.access.key", s3AccessKey)
      _sqlContext.sparkContext.hadoopConfiguration.set("fs.s3n.secret.key", s3SecretKey)
    }
    _sqlContext
  }

  lazy val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

}
