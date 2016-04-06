package de.frosner.ddq.testutils

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.test.TestHive

trait SparkContexts {

  protected val hive = SparkContexts.hive
  protected val sc = SparkContexts.sc
  protected val sql = SparkContexts.sql

}

object SparkContexts {

  private val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("DDQ"))
  private val sql = new SQLContext(sc)
  private val hive = new HiveContext(sc)
  hive.setConf("spark.sql.shuffle.partitions", "5")
  sql.setConf("spark.sql.shuffle.partitions", "5")
  println(s"Testing against Spark ${sc.version}")

}
