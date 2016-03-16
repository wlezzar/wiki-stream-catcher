package net.lezzar.wikistream

import net.lezzar.wikistream.tools.Logging
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}





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
