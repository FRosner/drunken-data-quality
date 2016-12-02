package de.frosner.ddq.testutils

import org.apache.spark.sql.SparkSession

trait SparkContexts {

  protected val spark: SparkSession =
    SparkSession.builder
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", "5")
      .appName("Testing")
      .getOrCreate()



  def resetSpark() = {

  }

  sys.addShutdownHook(resetSpark())

}


