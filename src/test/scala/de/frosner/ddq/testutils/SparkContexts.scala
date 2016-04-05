package de.frosner.ddq.testutils

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.test.TestHive

trait SparkContexts {

  protected val hive = SparkContexts.hive
  protected val sc = SparkContexts.sc
  protected val sql = SparkContexts.sql

}

object SparkContexts {

  private val hive = TestHive
  hive.setConf("spark.sql.shuffle.partitions", "5")
  private val sc = hive.sparkContext
  private val sql = new SQLContext(sc)
  sql.setConf("spark.sql.shuffle.partitions", "5")
  sys.addShutdownHook(hive.reset())
  println(s"Testing against Spark ${sc.version}")

}
