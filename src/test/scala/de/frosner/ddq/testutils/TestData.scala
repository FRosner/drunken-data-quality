package de.frosner.ddq.testutils

import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, DataFrame, SQLContext}

object TestData {

  def makeIntegerDf(sql: SQLContext, numbers: Seq[Int]): DataFrame =
    sql.createDataFrame(
      sql.sparkContext.makeRDD(numbers.map(Row(_))),
      StructType(List(StructField("column", IntegerType, false)))
    )

  def makeNullableStringDf(sql: SQLContext, strings: Seq[String]): DataFrame =
    sql.createDataFrame(sql.sparkContext.makeRDD(strings.map(Row(_))), StructType(List(StructField("column", StringType, true))))

  def makeIntegersDf(sql: SQLContext, row1: Seq[Int], rowN: Seq[Int]*): DataFrame = {
    val rows = row1 :: rowN.toList
    val numCols = row1.size
    val rdd = sql.sparkContext.makeRDD(rows.map(Row(_:_*)))
    val schema = StructType((1 to numCols).map(idx => StructField("column" + idx, IntegerType, false)))
    sql.createDataFrame(rdd, schema)
  }

}
