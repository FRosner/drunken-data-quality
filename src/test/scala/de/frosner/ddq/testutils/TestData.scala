package de.frosner.ddq.testutils

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object TestData {

  def makeIntegerDf(spark: SparkSession, numbers: Seq[Int]): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.makeRDD(numbers.map(Row(_))),
      StructType(List(StructField("column", IntegerType, nullable = false)))
    )

  def makeNullableStringDf(spark: SparkSession, strings: Seq[String]): DataFrame =
    spark.createDataFrame(spark.sparkContext.makeRDD(strings.map(Row(_))), StructType(List(StructField("column", StringType, nullable = true))))

  def makeIntegersDf(spark: SparkSession, row1: Seq[Int], rowN: Seq[Int]*): DataFrame = {
    val rows = row1 :: rowN.toList
    val numCols = row1.size
    val rdd = spark.sparkContext.makeRDD(rows.map(Row(_:_*)))
    val schema = StructType((1 to numCols).map(idx => StructField("column" + idx, IntegerType, nullable = false)))
    spark.createDataFrame(rdd, schema)
  }

}
