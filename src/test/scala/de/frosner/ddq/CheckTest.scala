package de.frosner.ddq

import java.text.SimpleDateFormat

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}

class CheckTest extends FlatSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  private val sc = new SparkContext("local[1]", "CheckTest")
  private val sql = new SQLContext(sc)
  private val hive = new TestHiveContext(sc)

  override def afterAll(): Unit = {
    hive.deletePaths()
  }

  private def makeIntegerDf(numbers: Seq[Int], sql: SQLContext): DataFrame =
    sql.createDataFrame(sc.makeRDD(numbers.map(Row(_))), StructType(List(StructField("column", IntegerType, false))))

  private def makeIntegerDf(numbers: Seq[Int]): DataFrame = makeIntegerDf(numbers, sql)

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

  "A joinable check" should "succeed if a join on the given column yields at least one row" in {
    val base = makeIntegerDf(List(1, 1, 1, 2, 2, 3))
    val ref = makeIntegerDf(List(1, 2, 5))
    Check(base).isJoinableWith(ref, "column" -> "column").run shouldBe true
  }

  it should "succeed if a join on the given columns yields at least one row" in {
    val base = makeIntegersDf(List(1, 2, 3), List(1, 2, 5), List(1, 3, 3))
    val ref = makeIntegersDf(List(1, 2, 100), List(1, 5, 100))
    Check(base).isJoinableWith(ref, "column1" -> "column1", "column2" -> "column2").run shouldBe true
  }

  it should "succeed if a join on the given columns yields at least one row if the columns have a different name" in {
    val base = makeIntegersDf(List(1, 2, 5), List(1, 2, 5), List(1, 100, 3))
    val ref = makeIntegersDf(List(1, 3, 100), List(1, 500, 100))
    Check(base).isJoinableWith(ref, "column1" -> "column1", "column3" -> "column2").run shouldBe true
  }

  it should "fail if a join on the given columns yields no result" in {
    val base = makeIntegersDf(List(1, 2, 5), List(1, 2, 5), List(1, 100, 3))
    val ref = makeIntegersDf(List(1, 1, 100), List(1, 10, 100))
    Check(base).isJoinableWith(ref, "column1" -> "column1", "column3" -> "column2").run shouldBe false
  }

  it should "fail if a join on the given columns is not possible due to mismatching types" in {
    val base = makeNullableStringDf(List("a", "b"))
    val ref = makeIntegerDf(List(1, 2, 3))
    Check(base).isJoinableWith(ref, "column" -> "column").run shouldBe false
  }

  "A check if a column is in the given values" should "succeed if all values are inside" in {
    Check(makeNullableStringDf(List("a", "b", "c", "c"))).isAnyOf("column", Set("a", "b", "c", "d")).run shouldBe true
  }

  it should "succeed if all values are inside or null" in {
    Check(makeNullableStringDf(List("a", "b", "c", null))).isAnyOf("column", Set("a", "b", "c", "d")).run shouldBe true
  }

  it should "fail if there are values not inside" in {
    Check(makeNullableStringDf(List("a", "b", "c", "c"))).isAnyOf("column", Set("a", "b", "d")).run shouldBe false
  }

  "A check if a column satisfies the given regex" should "succeed if all values satisfy the regex" in {
    Check(makeNullableStringDf(List("Hello A", "Hello B", "Hello C"))).isMatchingRegex("column", "^Hello").run shouldBe true
  }

  it should "succeed if all values satisfy the regex or are null" in {
    Check(makeNullableStringDf(List("Hello A", "Hello B", null))).isMatchingRegex("column", "^Hello").run shouldBe true
  }

  it should "fail if there are values not satisfying the regex" in {
    Check(makeNullableStringDf(List("Hello A", "Hello B", "Hello C"))).isMatchingRegex("column", "^Hello A$").run shouldBe false
  }

  "Multiple checks" should "fail if one check is failing" in {
    Check(makeIntegerDf(List(1,2,3))).hasNumRowsEqualTo(3).hasNumRowsEqualTo(2).run shouldBe false
  }

  it should "succeed if all checks are succeeding" in {
    Check(makeIntegerDf(List(1,2,3))).hasNumRowsEqualTo(3).hasUniqueKey("column").satisfies("column > 0").run shouldBe true
  }

  "It" should "be possible to specify a display name for a data frame" in {
    Check(makeIntegerDf(List(1,2,3)), displayName = Option("Integer Data Frame")).run
  }

  "A check from a SQLContext" should "load the given table" in {
    val df = makeIntegerDf(List(1,2,3))
    val tableName = "myintegerdf1"
    df.registerTempTable(tableName)
    Check.sqlTable(sql, tableName).hasNumRowsEqualTo(3).run shouldBe true
  }

  it should "require the table to exist" in {
    intercept[IllegalArgumentException] {
      Check.sqlTable(sql, "doesnotexist").run
    }
  }

  "A check from a HiveContext" should "load the given table from the given database" in {
    val tableName = "myintegerdf2"
    val databaseName = "testDb"
    hive.sql(s"CREATE DATABASE $databaseName")
    hive.sql(s"USE $databaseName")
    val df = makeIntegerDf(List(1,2,3), hive)
    df.registerTempTable(tableName)
    hive.sql(s"USE default")
    Check.hiveTable(hive, databaseName, tableName).hasNumRowsEqualTo(3).run shouldBe true
    hive.sql(s"DROP DATABASE $databaseName")
  }

  it should "require the table to exist" in {
    intercept[IllegalArgumentException] {
      Check.hiveTable(hive, "default", "doesnotexist").run
    }
  }

}
