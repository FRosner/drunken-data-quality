package de.frosner.ddq.testutils

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.test.TestHive

trait SparkContexts {

  protected val hive = TestHive
  hive.setConf("spark.sql.shuffle.partitions", "5")
  protected val sc = hive.sparkContext
  protected val sql = new SQLContext(sc)
  sql.setConf("spark.sql.shuffle.partitions", "5")
  sys.addShutdownHook(hive.reset())

}
