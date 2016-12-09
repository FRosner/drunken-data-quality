package de.frosner.ddq.testutils

import java.io.File

import org.apache.spark.sql.SparkSession

trait SparkContexts {

  protected val spark: SparkSession =
    SparkSession.builder
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", "5")
      .appName("Testing")
      .getOrCreate()


  sys.addShutdownHook(resetSpark())



  def resetSpark() = {
    new File("spark-warehouse").delete()
  }

}


