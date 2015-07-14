package de.frosner.ddq

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.scalatest.{FlatSpec, Matchers}

class CheckTest extends FlatSpec with Matchers {

  private val sc = new SparkContext("local[1]", "CheckTest")
  private val sql = new SQLContext(sc)

  private def makeIntegerDf(numbers: Seq[Int]): DataFrame =
    sql.createDataFrame(sc.makeRDD(numbers.map(Row(_))), StructType(List(StructField("column", IntegerType, false))))

  private def makeIntegersDf(row1: Seq[Int], rowN: Seq[Int]*): DataFrame = {
    val rows = (row1 :: rowN.toList)
    val numCols = row1.size
    val rdd = sc.makeRDD(rows.map(Row(_:_*)))
    val schema = StructType((1 to numCols).map(idx => StructField("column" + idx, IntegerType, false)))
    sql.createDataFrame(rdd, schema)
  }

  "A number of rows equality check" should "succeed if the number of rows is equal to the expected" in {
    Check(makeIntegerDf(List(1, 2, 3))).hasNumRowsEqualTo(3).run shouldBe true
  }

  it should "fail if the number of rows is not equal to the expected" in {
    Check(makeIntegerDf(List(1, 2, 3))).hasNumRowsEqualTo(4).run shouldBe false
  }

  "A satisfies check" should "succeed if all rows satisfy the given condition" in {
    Check(makeIntegerDf(List(1, 2, 3))).satisfies("column > 0").run shouldBe true
  }

  it should "fail if there are rows that do not satisfy the given condition" in {
    Check(makeIntegerDf(List(1, 2, 3))).satisfies("column > 1").run shouldBe false
  }

  "A key check" should "succeed if a given column defines a key" in {
    val df = makeIntegersDf(
      List(1,2),
      List(2,3),
      List(3,3)
    )
    Check(df).hasKey("column1").run shouldBe true
  }

  it should "succeed if the given columns define a key" in {
    val df = makeIntegersDf(
      List(1,2,3),
      List(2,3,3),
      List(3,2,3)
    )
    Check(df).hasKey("column1", "column2").run shouldBe true
  }

  it should "fail if there are duplicate rows using the given column as a key" in {
    val df = makeIntegersDf(
      List(1,2),
      List(2,3),
      List(2,3)
    )
    Check(df).hasKey("column1").run shouldBe false
  }

  it should "fail if there are duplicate rows using the given columns as a key" in {
    val df = makeIntegersDf(
      List(1,2,3),
      List(2,3,3),
      List(1,2,3)
    )
    Check(df).hasKey("column1", "column2").run shouldBe false
  }

  "Multiple checks" should "fail if one check is failing" in {
    Check(makeIntegerDf(List(1,2,3))).hasNumRowsEqualTo(3).hasNumRowsEqualTo(2).run shouldBe false
  }

  it should "succeed if all checks are succeeding" in {
    Check(makeIntegerDf(List(1,2,3))).hasNumRowsEqualTo(3).hasKey("column").satisfies("column > 0").run shouldBe true
  }

}
