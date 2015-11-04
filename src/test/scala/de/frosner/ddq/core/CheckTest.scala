package de.frosner.ddq.core

import java.io.{FileOutputStream, FileDescriptor, PrintStream, ByteArrayOutputStream}
import java.text.SimpleDateFormat

import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SQLContext}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}

import de.frosner.ddq.reporters.{ConsoleReporter, Reporter}

class CheckTest extends FlatSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with MockitoSugar {

  private val sc = new SparkContext("local[1]", "CheckTest")
  private val sql = new SQLContext(sc)
  sql.setConf("spark.sql.shuffle.partitions", "5")
  private val hive = new TestHiveContext(sc)
  hive.setConf("spark.sql.shuffle.partitions", "5")

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

  "Multiple checks" should "produce a constraintResults map with all constraints and corresponding results" in {
    val check = Check(makeIntegerDf(List(1,2,3))).hasNumRowsEqualTo(3).hasNumRowsEqualTo(2).satisfies("column > 0")
    val constraint1 = check.constraints(0)
    val constraint2 = check.constraints(1)
    val constraint3 = check.constraints(2)

    check.run().constraintResults shouldBe Map(
      constraint1 -> ConstraintSuccess("The number of rows is equal to 3"),
      constraint2 -> ConstraintFailure("The actual number of rows 3 is not equal to the expected 2"),
      constraint3 -> ConstraintSuccess("Constraint column > 0 is satisfied")
    )
  }

  "A check from a SQLContext" should "load the given table" in {
    val df = makeIntegerDf(List(1,2,3), sql)
    val tableName = "myintegerdf1"
    df.registerTempTable(tableName)
    val constraint = Check.hasNumRowsEqualTo(3)
    val result = ConstraintSuccess("The number of rows is equal to 3")
    Check.sqlTable(sql, tableName).addConstraint(constraint).run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "require the table to exist" in {
    intercept[IllegalArgumentException] {
      Check.sqlTable(sql, "doesnotexist").run()
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
    val constraint = Check.hasNumRowsEqualTo(3)
    val result = ConstraintSuccess("The number of rows is equal to 3")
    Check.hiveTable(hive, databaseName, tableName).addConstraint(constraint).run().
      constraintResults shouldBe Map(constraint -> result)
    hive.sql(s"DROP DATABASE $databaseName")
  }

  it should "require the table to exist" in {
    intercept[IllegalArgumentException] {
      Check.hiveTable(hive, "default", "doesnotexist").run()
    }
  }

  "The run method on a Check" should "work correctly when multiple reporters are specified" in {
    val df = mock[DataFrame]
    when(df.toString).thenReturn("")
    when(df.count).thenReturn(1)
    when(df.columns).thenReturn(Array.empty[String])

    val reporter1 = mock[Reporter]
    val reporter2 = mock[Reporter]

    val constraints = Seq.empty[Constraint]
    val check = Check(df, None, None, constraints)
    val result = check.run(reporter1, reporter2)

    result.check shouldBe check
    result.constraintResults shouldBe Map.empty

    verify(reporter1).report(result)
    verify(reporter2).report(result)
  }

  it should "work correctly when a single reporter is specified" in {
    val df = mock[DataFrame]
    when(df.toString).thenReturn("")
    when(df.count).thenReturn(1)
    when(df.columns).thenReturn(Array.empty[String])

    val reporter = mock[Reporter]

    val constraints = Seq.empty[Constraint]
    val check = Check(df, None, None, constraints)
    val result = check.run(reporter)

    result.check shouldBe check
    result.constraintResults shouldBe Map.empty

    verify(reporter).report(result)
  }

  it should "use the console reporter if no reporter is specified" in {
    val df = mock[DataFrame]
    when(df.toString).thenReturn("")
    when(df.count).thenReturn(1)
    when(df.columns).thenReturn(Array.empty[String])

    val defaultBaos = new ByteArrayOutputStream()
    System.setOut(new PrintStream(defaultBaos))

    val consoleBaos = new ByteArrayOutputStream()
    val consoleReporter = new ConsoleReporter(new PrintStream(consoleBaos))

    val constraints = Seq.empty[Constraint]
    val check = Check(df, None, None, constraints)
    val result = check.run()
    check.run(consoleReporter)

    result.check shouldBe check
    result.constraintResults shouldBe Map.empty
    defaultBaos.toString shouldBe consoleBaos.toString

    // reset System.out
    System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)))
  }

  "A number of rows equality check" should "" in {
    val check = Check(makeIntegerDf(List(1, 2, 3))).hasNumRowsEqualTo(3)
    val constraint = check.constraints.head
    val result = ConstraintSuccess("The number of rows is equal to 3")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if the number of rows is not equal to the expected" in {
    val check = Check(makeIntegerDf(List(1, 2, 3))).hasNumRowsEqualTo(4)
    val constraint = check.constraints.head
    val result = ConstraintFailure("The actual number of rows 3 is not equal to the expected 4")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "A satisfies check (String)" should "succeed if all rows satisfy the given condition" in {
    val check = Check(makeIntegerDf(List(1, 2, 3))).satisfies("column > 0")
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Constraint column > 0 is satisfied")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if there are rows that do not satisfy the given condition" in {
    val check = Check(makeIntegerDf(List(1, 2, 3))).satisfies("column > 1")
    val constraint = check.constraints.head
    val result = ConstraintFailure("One row did not satisfy constraint column > 1")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "A satisfies check (Column)" should "succeed if all rows satisfy the given condition" in {
    val check = Check(makeIntegerDf(List(1, 2, 3))).satisfies(new Column("column") > 0)
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Constraint (column > 0) is satisfied")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if there is a row that does not satisfy the given condition" in {
    val check = Check(makeIntegerDf(List(1, 2, 3))).satisfies(new Column("column") > 1)
    val constraint = check.constraints.head
    val result = ConstraintFailure("One row did not satisfy constraint (column > 1)")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if there are rows that do not satisfy the given condition" in {
    val check = Check(makeIntegerDf(List(-1, 0, 1, 2, 3))).satisfies(new Column("column") > 1)
    val constraint = check.constraints.head
    val result = ConstraintFailure("3 rows did not satisfy constraint (column > 1)")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "A conditional satisfies check" should "succeed if all rows where the statement is true, satisfy the given condition" in {
    val check = Check(makeIntegersDf(
      List(1, 0),
      List(2, 0),
      List(3, 0)
    )).satisfies((new Column("column1") === 1) -> (new Column("column2") === 0))
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Constraint (column1 = 1) -> (column2 = 0) is satisfied")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if there are no rows where the statement is true" in {
    val check = Check(makeIntegersDf(
      List(1, 0),
      List(1, 1),
      List(3, 0)
    )).satisfies((new Column("column1") === 5) -> (new Column("column2") === 100))
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Constraint (column1 = 5) -> (column2 = 100) is satisfied")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if there are rows that do not satisfy the given condition" in {
    val check = Check(makeIntegersDf(
      List(1, 0),
      List(1, 1),
      List(3, 0)
    )).satisfies((new Column("column1") === 1) -> (new Column("column2") === 0))
    val constraint = check.constraints.head
    val result = ConstraintFailure("One row did not satisfy constraint (column1 = 1) -> (column2 = 0)")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "A unique key check" should "succeed if a given column defines a key" in {
    val df = makeIntegersDf(
      List(1,2),
      List(2,3),
      List(3,3)
    )
    val check = Check(df).hasUniqueKey("column1")
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Column column1 is a key")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if the given columns define a key" in {
    val df = makeIntegersDf(
      List(1,2,3),
      List(2,3,3),
      List(3,2,3)
    )
    val check = Check(df).hasUniqueKey("column1", "column2")
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Columns column1,column2 are a key")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if there are duplicate rows using the given column as a key" in {
    val df = makeIntegersDf(
      List(1,2),
      List(2,3),
      List(2,3)
    )
    val check = Check(df).hasUniqueKey("column1")
    val constraint = check.constraints.head
    val result = ConstraintFailure("Column column1 is not a key")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if there are duplicate rows using the given columns as a key" in {
    val df = makeIntegersDf(
      List(1,2,3),
      List(2,3,3),
      List(1,2,3)
    )
    val check = Check(df).hasUniqueKey("column1", "column2")
    val constraint = check.constraints.head
    val result = ConstraintFailure("Columns column1,column2 are not a key")

    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "An is-always-null check" should "succeed if the column is always null" in {
    val check = Check(makeNullableStringDf(List(null, null, null))).isAlwaysNull("column")
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Column column is null")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if the column is not always null" in {
    val check = Check(makeNullableStringDf(List("a", null, null))).isAlwaysNull("column")
    val constraint = check.constraints.head
    val result = ConstraintFailure("Column column has one non-null row although it should be null")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "An is-never-null check" should "succeed if the column contains no null values" in {
    val check = Check(makeNullableStringDf(List("a", "b", "c"))).isNeverNull("column")
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Column column is not null")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if the column contains null values" in {
    val check = Check(makeNullableStringDf(List("a", "b", null))).isNeverNull("column")
    val constraint = check.constraints.head
    val result = ConstraintFailure("Column column has one null row although it should not be null")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "A to Int conversion check" should "succeed if all elements can be converted to Int" in {
    val check = Check(makeNullableStringDf(List("1", "2", "3"))).isConvertibleToInt("column")
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Column column can be converted to Int")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if all elements can be converted to Int or are null" in {
    val check = Check(makeNullableStringDf(List("1", "2", null))).isConvertibleToInt("column")
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Column column can be converted to Int")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if one element cannot be converted to Int" in {
    val check = Check(makeNullableStringDf(List("1", "hallo", "3"))).isConvertibleToInt("column")
    val constraint = check.constraints.head
    val result = ConstraintFailure("Column column contains one row that cannot be converted to Int")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if multiple elements cannot be converted to Int" in {
    val check = Check(makeNullableStringDf(List("1", "hallo", "3", "frank"))).isConvertibleToInt("column")
    val constraint = check.constraints.head
    val result = ConstraintFailure("Column column contains 2 rows that cannot be converted to Int")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "A to Double conversion check" should "succeed if all elements can be converted to Double" in {
    val check = Check(makeNullableStringDf(List("1.0", "2.0", "3"))).isConvertibleToDouble("column")
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Column column can be converted to Double")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if all elements can be converted to Double or are null" in {
    val check = Check(makeNullableStringDf(List("1", "2.0", null))).isConvertibleToDouble("column")
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Column column can be converted to Double")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if at least one element cannot be converted to Double" in {
    val check = Check(makeNullableStringDf(List("1", "hallo", "3"))).isConvertibleToDouble("column")
    val constraint = check.constraints.head
    val result = ConstraintFailure("Column column contains one row that cannot be converted to Double")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "A to Long conversion check" should "succeed if all elements can be converted to Long" in {
    val check = Check(makeNullableStringDf(List("1", "2", "34565465756776"))).isConvertibleToLong("column")
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Column column can be converted to Long")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if all elements can be converted to Long or are null" in {
    val check =  Check(makeNullableStringDf(List("1", "2", null))).isConvertibleToLong("column")
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Column column can be converted to Long")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if one element cannot be converted to Long" in {
    val check = Check(makeNullableStringDf(List("1", "hallo", "3"))).isConvertibleToLong("column")
    val constraint = check.constraints.head
    val result = ConstraintFailure("Column column contains one row that cannot be converted to Long")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if several elements cannot be converted to Long" in {
    val check = Check(makeNullableStringDf(List("1", "hallo", "3", "frank"))).isConvertibleToLong("column")
    val constraint = check.constraints.head
    val result = ConstraintFailure("Column column contains 2 rows that cannot be converted to Long")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "A to Date conversion check" should "succeed if all elements can be converted to Date" in {
    val check = Check(makeNullableStringDf(List("2000-11-23 11:50:10", "2000-5-23 11:50:10", "2000-02-23 11:11:11"))).
      isConvertibleToDate("column", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"))
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Column column can be converted to Date")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if all elements can be converted to Date or are null" in {
    val check = Check(makeNullableStringDf(List("2000-11-23 11:50:10", null, "2000-02-23 11:11:11"))).
      isConvertibleToDate("column", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"))
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Column column can be converted to Date")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if at least one element cannot be converted to Date" in {
    val check =  Check(makeNullableStringDf(List("2000-11-23 11:50:10", "abc", "2000-15-23 11:11:11"))).
      isConvertibleToDate("column", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"))
    val constraint = check.constraints.head
    val result = ConstraintFailure("Column column contains one row that cannot be converted to Date")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "A foreign key check" should "succeed if the given column is a foreign key pointing to the reference table" in {
    val base = makeIntegerDf(List(1, 1, 1, 2, 2, 3))
    val ref = makeIntegerDf(List(1, 2, 3))
    val check = Check(base).hasForeignKey(ref, "column" -> "column")
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Column column->column defines a foreign key")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if the given columns are a foreign key pointing to the reference table" in {
    val base = makeIntegersDf(List(1, 2, 3), List(1, 2, 5), List(1, 3, 3))
    val ref = makeIntegersDf(List(1, 2, 100), List(1, 3, 100))
    val check = Check(base).hasForeignKey(ref, "column1" -> "column1", "column2" -> "column2")
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Columns column1->column1, column2->column2 define a foreign key")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if the given columns are a foreign key pointing to the reference table having a different name" in {
    val base = makeIntegersDf(List(1, 2, 3), List(1, 2, 5), List(1, 3, 3))
    val ref = makeIntegersDf(List(1, 3, 100), List(1, 5, 100))
    val check = Check(base).hasForeignKey(ref, "column1" -> "column1", "column3" -> "column2")
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Columns column1->column1, column3->column2 define a foreign key")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if the given column contains values that are not found in the reference table" in {
    val base = makeIntegerDf(List(1, 1, 1, 2, 2, 3))
    val ref = makeIntegerDf(List(1, 2))
    val check = Check(base).hasForeignKey(ref, "column" -> "column")
    val constraint = check.constraints.head
    val result = ConstraintFailure("Column column->column does not define a foreign key (one record does not match)")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if the given columns contains values that are not found in the reference table" in {
    val base = makeIntegersDf(List(1, 2, 3), List(1, 2, 5), List(1, 5, 3))
    val ref = makeIntegersDf(List(1, 2, 100), List(1, 3, 100))
    val check = Check(base).hasForeignKey(ref, "column1" -> "column1", "column2" -> "column2")
    val constraint = check.constraints.head
    val result = ConstraintFailure("Columns column1->column1, column2->column2 do not define a foreign key (one record does not match)")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if the foreign key is not a primary key in the reference table" in {
    val base = makeIntegersDf(List(1, 2, 3), List(1, 2, 5), List(1, 3, 3))
    val ref = makeIntegersDf(List(1, 3, 100), List(1, 5, 100), List(1, 5, 500))
    val check = Check(base).hasForeignKey(ref, "column1" -> "column1", "column3" -> "column2")
    val constraint = check.constraints.head
    val result = ConstraintFailure("Columns column1, column2 are not a key in reference table")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "A joinable check" should "succeed if a join on the given column yields at least one row" in {
    val base = makeIntegerDf(List(1, 1, 1, 2, 2, 3))
    val ref = makeIntegerDf(List(1, 2, 5))
    val check = Check(base).isJoinableWith(ref, "column" -> "column")
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Column column->column can be used for joining (number of distinct rows in base table: 3, number of distinct rows after joining: 2, merge rate: 0.67)")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if a join on the given columns yields at least one row" in {
    val base = makeIntegersDf(List(1, 2, 3), List(1, 2, 5), List(1, 3, 3))
    val ref = makeIntegersDf(List(1, 2, 100), List(1, 5, 100))
    val check = Check(base).isJoinableWith(ref, "column1" -> "column1", "column2" -> "column2")
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Columns column1->column1, column2->column2 can be used for joining (number of distinct rows in base table: 2, number of distinct rows after joining: 1, merge rate: 0.50)")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if a join on the given columns yields at least one row if the columns have a different name" in {
    val base = makeIntegersDf(List(1, 2, 5), List(1, 2, 5), List(1, 100, 3))
    val ref = makeIntegersDf(List(1, 3, 100), List(1, 500, 100))
    val check = Check(base).isJoinableWith(ref, "column1" -> "column1", "column3" -> "column2")
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Columns column1->column1, column3->column2 can be used for joining (number of distinct rows in base table: 2, number of distinct rows after joining: 1, merge rate: 0.50)")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if a join on the given columns yields no result" in {
    val base = makeIntegersDf(List(1, 2, 5), List(1, 2, 5), List(1, 100, 3))
    val ref = makeIntegersDf(List(1, 1, 100), List(1, 10, 100))
    val check = Check(base).isJoinableWith(ref, "column1" -> "column1", "column3" -> "column2")
    val constraint = check.constraints.head
    val result = ConstraintFailure("Columns column1->column1, column3->column2 cannot be used for joining (no rows match)")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if a join on the given columns is not possible due to mismatching types" in {
    val base = makeNullableStringDf(List("a", "b"))
    val ref = makeIntegerDf(List(1, 2, 3))
    val check = Check(base).isJoinableWith(ref, "column" -> "column")
    val constraint = check.constraints.head
    val result = ConstraintFailure("Column column->column cannot be used for joining (no rows match)")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "A check if a column is in the given values" should "succeed if all values are inside" in {
    val check = Check(makeNullableStringDf(List("a", "b", "c", "c"))).isAnyOf("column", Set("a", "b", "c", "d"))
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Column column contains only values in Set(a, b, c, d)")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if all values are inside or null" in {
    val check = Check(makeNullableStringDf(List("a", "b", "c", null))).isAnyOf("column", Set("a", "b", "c", "d"))
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Column column contains only values in Set(a, b, c, d)")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if there are values not inside" in {
    val check = Check(makeNullableStringDf(List("a", "b", "c", "c"))).isAnyOf("column", Set("a", "b", "d"))
    val constraint = check.constraints.head
    val result = ConstraintFailure("Column column contains 2 rows that are not in Set(a, b, d)")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "A check if a column satisfies the given regex" should "succeed if all values satisfy the regex" in {
    val check = Check(makeNullableStringDf(List("Hello A", "Hello B", "Hello C"))).isMatchingRegex("column", "^Hello")
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Column column matches ^Hello")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if all values satisfy the regex or are null" in {
    val check = Check(makeNullableStringDf(List("Hello A", "Hello B", null))).isMatchingRegex("column", "^Hello")
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Column column matches ^Hello")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if there is a single row not satisfying the regex" in {
    val check = Check(makeNullableStringDf(List("Hello A", "Hello A", "Hello B"))).isMatchingRegex("column", "^Hello A$")
    val constraint = check.constraints.head
    val result = ConstraintFailure("Column column contains one row that does not match ^Hello A$")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if there are multiple rows not satisfying the regex" in {
    val check = Check(makeNullableStringDf(List("Hello A", "Hello B", "Hello C"))).isMatchingRegex("column", "^Hello A$")
    val constraint = check.constraints.head
    val result = ConstraintFailure("Column column contains 2 rows that do not match ^Hello A$")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "A to boolean conversion check" should "succeed if column values are true and false only" in {
    val check = Check(makeNullableStringDf(List("true", "false"))).isConvertibleToBoolean("column")
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Column column can be converted to Boolean")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if column values are not true and false only" in {
    val check = Check(makeNullableStringDf(List("true", "false", "error"))).isConvertibleToBoolean("column")
    val constraint = check.constraints.head
    val result = ConstraintFailure("Column column contains one row that cannot be converted to Boolean")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if column values are true/TRUE and false/FALSE if case sensitive is false" in {
    val df = makeNullableStringDf(List("true", "false", "TRUE", "FALSE", "True", "fAlsE"))
    val check = Check(df).isConvertibleToBoolean("column", isCaseSensitive = false)
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Column column can be converted to Boolean")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if column values are true/TRUE and false/FALSE if case sensitive is true" in {
    val df = makeNullableStringDf(List("true", "false", "TRUE", "FALSE", "True", "fAlsE"))
    val check = Check(df).isConvertibleToBoolean("column", isCaseSensitive = true)
    val constraint = check.constraints.head
    val result = ConstraintFailure("Column column contains 4 rows that cannot be converted to Boolean")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if column values are 1 and 0 only" in {
    val check = Check(makeNullableStringDf(List("1", "0"))).isConvertibleToBoolean("column", "1", "0")
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Column column can be converted to Boolean")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if column values are not 1 and 0 only" in {
    val check = Check(makeNullableStringDf(List("1", "0", "2"))).isConvertibleToBoolean("column", "1", "0")
    val constraint = check.constraints.head
    val result = ConstraintFailure("Column column contains one row that cannot be converted to Boolean")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if column values are convertible or null" in {
    val check = Check(makeNullableStringDf(List("1", "0", null))).isConvertibleToBoolean("column", "1", "0")
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Column column can be converted to Boolean")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "A functional dependency check" should "succeed if the column values in the determinant set always correspond to the column values in the dependent set" in {
    val check = Check(makeIntegersDf(
      List(1, 2, 1, 1),
      List(9, 9, 9, 2),
      List(9, 9, 9, 3),
      List(3, 4, 3, 4),
      List(7, 7, 7, 5)
    )).hasFunctionalDependency(Seq("column1", "column2"), Seq("column3"))
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Columns [column3] are functionally dependent on [column1, column2]")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed also for dependencies where the determinant and dependent sets are not distinct" in {
    val check = Check(makeIntegersDf(
      List(1, 2, 3, 1),
      List(9, 9, 9, 2),
      List(9, 9, 9, 3),
      List(3, 4, 3, 4),
      List(7, 7, 7, 5)
    )).hasFunctionalDependency(Seq("column1", "column2"), Seq("column2", "column3"))
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Columns [column2, column3] are functionally dependent on [column1, column2]")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if the determinant and dependent sets are equal" in {
    val check = Check(makeIntegerDf(
      List(1, 2, 3)
    )).hasFunctionalDependency(Seq("column"), Seq("column"))
    val constraint = check.constraints.head
    val result = ConstraintSuccess("Columns [column] are functionally dependent on [column]")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if the column values in the determinant set don't always correspond to the column values in the dependent set" in {
    val check = Check(makeIntegersDf(
      List(1, 2, 1, 1),
      List(9, 9, 9, 1),
      List(9, 9, 8, 1),
      List(3, 4, 3, 1),
      List(7, 7, 7, 1),
      List(7, 7, 6, 1)
    )).hasFunctionalDependency(Seq("column1", "column2"), Seq("column3"))
    val constraint = check.constraints.head
    val result = ConstraintFailure("Columns [column3] are not functionally dependent on [column1, column2] " +
      "(2 violating determinant values)")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail also for dependencies where the determinant and dependent sets are not distinct" in {
    val check = Check(makeIntegersDf(
      List(1, 2, 1, 1),
      List(9, 9, 9, 1),
      List(9, 9, 8, 1),
      List(3, 4, 3, 1),
      List(7, 7, 7, 1)
    )).hasFunctionalDependency(Seq("column1", "column2"), Seq("column2", "column3"))
    val constraint = check.constraints.head
    val result = ConstraintFailure("Columns [column2, column3] are not functionally dependent on [column1, column2] " +
      "(1 violating determinant values)")
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "require the determinant set to be non-empty" in {
    intercept[IllegalArgumentException] {
      Check(makeIntegersDf(
        List(1, 2, 3),
        List(4, 5, 6)
      )).hasFunctionalDependency(Seq.empty, Seq("column0"))
    }
  }

  it should "require the dependent set to be non-empty" in {
    intercept[IllegalArgumentException] {
      Check(makeIntegersDf(
        List(1, 2, 3),
        List(4, 5, 6)
      )).hasFunctionalDependency(Seq("column0"), Seq.empty)
    }
  }

}
