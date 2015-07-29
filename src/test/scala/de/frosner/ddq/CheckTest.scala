package de.frosner.ddq

import java.text.SimpleDateFormat

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.scalatest.{Tag, FlatSpec, Matchers}

class CheckTest extends FlatSpec with Matchers {

  private val sc = new SparkContext("local[1]", "CheckTest")
  private val sql = new SQLContext(sc)

  sql.setConf("spark.sql.shuffle.partitions","1")

  private def makeIntegerDf(numbers: Seq[Int]): DataFrame =
    sql.createDataFrame(sc.makeRDD(numbers.map(Row(_))), StructType(List(StructField("column", IntegerType, false))))

  private def makeNullableStringDf(strings: Seq[String]): DataFrame =
    sql.createDataFrame(sc.makeRDD(strings.map(Row(_))), StructType(List(StructField("column", StringType, true))))

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

  "A unique key check" should "succeed if a given column defines a key" in {
    val df = makeIntegersDf(
      List(1,2),
      List(2,3),
      List(3,3)
    )
    Check(df).hasUniqueKey("column1").run shouldBe true
  }

  it should "succeed if the given columns define a key" in {
    val df = makeIntegersDf(
      List(1,2,3),
      List(2,3,3),
      List(3,2,3)
    )
    Check(df).hasUniqueKey("column1", "column2").run shouldBe true
  }

  it should "fail if there are duplicate rows using the given column as a key" in {
    val df = makeIntegersDf(
      List(1,2),
      List(2,3),
      List(2,3)
    )
    Check(df).hasUniqueKey("column1").run shouldBe false
  }

  it should "fail if there are duplicate rows using the given columns as a key" in {
    val df = makeIntegersDf(
      List(1,2,3),
      List(2,3,3),
      List(1,2,3)
    )
    Check(df).hasUniqueKey("column1", "column2").run shouldBe false
  }

  "An is-always-null check" should "succeed if the column is always null" in {
    Check(makeNullableStringDf(List(null, null, null))).isAlwaysNull("column").run shouldBe true
  }

  it should "fail if the column is not always null" in {
    Check(makeNullableStringDf(List("a", null, null))).isAlwaysNull("column").run shouldBe false
  }

  "An is-never-null check" should "succeed if the column contains no null values" in {
    Check(makeNullableStringDf(List("a", "b", "c"))).isNeverNull("column").run shouldBe true
  }

  it should "fail if the column contains null values" in {
    Check(makeNullableStringDf(List("a", "b", null))).isNeverNull("column").run shouldBe false
  }

  "A to Int conversion check" should "succeed if all elements can be converted to Int" in {
    Check(makeNullableStringDf(List("1", "2", "3"))).isConvertibleToInt("column").run shouldBe true
  }

  it should "succeed if all elements can be converted to Int or are null" in {
    Check(makeNullableStringDf(List("1", "2", null))).isConvertibleToInt("column").run shouldBe true
  }

  it should "fail if at least one element cannot be converted to Int" in {
    Check(makeNullableStringDf(List("1", "hallo", "3"))).isConvertibleToInt("column").run shouldBe false
  }

  "A to Double conversion check" should "succeed if all elements can be converted to Double" in {
    Check(makeNullableStringDf(List("1.0", "2.0", "3"))).isConvertibleToDouble("column").run shouldBe true
  }

  it should "succeed if all elements can be converted to Double or are null" in {
    Check(makeNullableStringDf(List("1", "2.0", null))).isConvertibleToDouble("column").run shouldBe true
  }

  it should "fail if at least one element cannot be converted to Double" in {
    Check(makeNullableStringDf(List("1", "hallo", "3"))).isConvertibleToDouble("column").run shouldBe false
  }

  "A to Long conversion check" should "succeed if all elements can be converted to Long" in {
    Check(makeNullableStringDf(List("1", "2", "34565465756776"))).isConvertibleToLong("column").run shouldBe true
  }

  it should "succeed if all elements can be converted to Long or are null" in {
    Check(makeNullableStringDf(List("1", "2", null))).isConvertibleToLong("column").run shouldBe true
  }

  it should "fail if at least one element cannot be converted to Long" in {
    Check(makeNullableStringDf(List("1", "hallo", "3"))).isConvertibleToLong("column").run shouldBe false
  }

  "A to Date conversion check" should "succeed if all elements can be converted to Date" in {
    Check(makeNullableStringDf(List("2000-11-23 11:50:10", "2000-5-23 11:50:10", "2000-02-23 11:11:11")))
      .isConvertibleToDate("column", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).run shouldBe true
  }

  it should "succeed if all elements can be converted to Date or are null" in {
    Check(makeNullableStringDf(List("2000-11-23 11:50:10", null, "2000-02-23 11:11:11")))
      .isConvertibleToDate("column", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).run shouldBe true
  }

  it should "fail if at least one element cannot be converted to Date" in {
    Check(makeNullableStringDf(List("2000-11-23 11:50:10", "abc", "2000-15-23 11:11:11")))
      .isConvertibleToDate("column", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).run shouldBe false
  }

  "A foreign key check" should "succeed if the given column is a foreign key pointing to the reference table" in {
    val base = makeIntegerDf(List(1, 1, 1, 2, 2, 3))
    val ref = makeIntegerDf(List(1, 2, 3))
    Check(base).hasForeignKey(ref, "column" -> "column").run shouldBe true
  }

  it should "succeed if the given columns are a foreign key pointing to the reference table" in {
    val base = makeIntegersDf(List(1, 2, 3), List(1, 2, 5), List(1, 3, 3))
    val ref = makeIntegersDf(List(1, 2, 100), List(1, 3, 100))
    Check(base).hasForeignKey(ref, "column1" -> "column1", "column2" -> "column2").run shouldBe true
  }

  it should "succeed if the given columns are a foreign key pointing to the reference table having a different name" in {
    val base = makeIntegersDf(List(1, 2, 3), List(1, 2, 5), List(1, 3, 3))
    val ref = makeIntegersDf(List(1, 3, 100), List(1, 5, 100))
    Check(base).hasForeignKey(ref, "column1" -> "column1", "column3" -> "column2").run shouldBe true
  }

  it should "fail if the given column contains values that are not found in the reference table" in {
    val base = makeIntegerDf(List(1, 1, 1, 2, 2, 3))
    val ref = makeIntegerDf(List(1, 2))
    Check(base).hasForeignKey(ref, "column" -> "column").run shouldBe false
  }

  it should "fail if the given columns contains values that are not found in the reference table" in {
    val base = makeIntegersDf(List(1, 2, 3), List(1, 2, 5), List(1, 5, 3))
    val ref = makeIntegersDf(List(1, 2, 100), List(1, 3, 100))
    Check(base).hasForeignKey(ref, "column1" -> "column1", "column2" -> "column2").run shouldBe false
  }

  it should "fail if the foreign key is not a primary key in the reference table" in {
    val base = makeIntegersDf(List(1, 2, 3), List(1, 2, 5), List(1, 3, 3))
    val ref = makeIntegersDf(List(1, 3, 100), List(1, 5, 100), List(1, 5, 500))
    Check(base).hasForeignKey(ref, "column1" -> "column1", "column3" -> "column2").run shouldBe false
  }

  "Multiple checks" should "fail if one check is failing" in {
    Check(makeIntegerDf(List(1,2,3))).hasNumRowsEqualTo(3).hasNumRowsEqualTo(2).run shouldBe false
  }

  it should "succeed if all checks are succeeding" in {
    Check(makeIntegerDf(List(1,2,3))).hasNumRowsEqualTo(3).hasUniqueKey("column").satisfies("column > 0").run shouldBe true
  }

  //isConvertibleToBoolean
  "A boolean check" should "succeed if column values are true and false only" in {
    Check(makeNullableStringDf(List("true","false"))).isConvertibleToBoolean("column").run shouldBe true
  }
  it should "fail if column values are not true and false only" in {
    Check(makeNullableStringDf(List("true","false","error"))).isConvertibleToBoolean("column").run shouldBe false
  }
  it should "succeed if column values are true/TRUE and false/FALSE if case sensitive is false" in {
    Check(makeNullableStringDf(List("true","false","TRUE","FALSE","True","fAlsE"))).isConvertibleToBoolean("column",isCaseSensitive = false).run shouldBe true
  }
  it should "succeed if column values are 1 and 0 only" in {
    Check(makeNullableStringDf(List("1","0"))).isConvertibleToBoolean("column","1","0").run shouldBe true
  }
  it should "fail if column values are not 1 and 0 only" in {
    Check(makeNullableStringDf(List("1","0","2"))).isConvertibleToBoolean("column","1","0").run shouldBe false
  }

}
