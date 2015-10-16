package de.frosner.ddq

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, BeforeAndAfterEach}


abstract class TestDataFrameContext extends FlatSpec with BeforeAndAfterEach with BeforeAndAfterAll{
  protected val sc = new SparkContext("local[1]", this.getClass.getSimpleName)
  protected val sql = new SQLContext(sc)
  sql.setConf("spark.sql.shuffle.partitions", "5")
  protected val hive = new TestHiveContext(sc)
  hive.setConf("spark.sql.shuffle.partitions", "5")

  override def afterAll(): Unit = {
    hive.deletePaths()
  }

  protected def makeIntegerDf(numbers: Seq[Int], sql: SQLContext): DataFrame =
    sql.createDataFrame(sc.makeRDD(numbers.map(Row(_))), StructType(List(StructField("column", IntegerType, false))))

  protected def makeIntegerDf(numbers: Seq[Int]): DataFrame = makeIntegerDf(numbers, sql)

  protected def makeNullableStringDf(strings: Seq[String]): DataFrame =
    sql.createDataFrame(sc.makeRDD(strings.map(Row(_))), StructType(List(StructField("column", StringType, true))))

  protected def makeIntegersDf(row1: Seq[Int], rowN: Seq[Int]*): DataFrame = {
    val rows = (row1 :: rowN.toList)
    val numCols = row1.size
    val rdd = sc.makeRDD(rows.map(Row(_:_*)))
    val schema = StructType((1 to numCols).map(idx => StructField("column" + idx, IntegerType, false)))
    sql.createDataFrame(rdd, schema)
  }
}
