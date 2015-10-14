package de.frosner.ddq

import java.text.SimpleDateFormat

import org.apache.spark.sql.Column
import org.scalatest. Matchers

class CheckTest extends TestDataFrameContext with Matchers {



  it should "fail if the number of rows is not equal to the expected" in {
    val expectedResult = List(ConstraintFailure("The actual number of rows 3 is not equal to the expected 4"))
    Check(makeIntegerDf(List(1, 2, 3))).hasNumRowsEqualTo(4).run().constraintResults shouldBe expectedResult
  }

  "A satisfies check (String)" should "succeed if all rows satisfy the given condition" in {
    val expectedResult = List(ConstraintSuccess("Constraint column > 0 is satisfied"))
    Check(makeIntegerDf(List(1, 2, 3))).satisfies("column > 0").run().constraintResults shouldBe expectedResult
  }

  it should "fail if there are rows that do not satisfy the given condition" in {
    val expectedResult = List(ConstraintFailure("1 rows did not satisfy constraint column > 1"))
    Check(makeIntegerDf(List(1, 2, 3))).satisfies("column > 1").run().constraintResults shouldBe expectedResult
  }

  "A satisfies check (Column)" should "succeed if all rows satisfy the given condition" in {
    val expectedResult = List(ConstraintSuccess("Constraint (column > 0) is satisfied"))
    Check(makeIntegerDf(List(1, 2, 3))).satisfies(new Column("column") > 0).run().
      constraintResults shouldBe expectedResult
  }

  it should "fail if there are rows that do not satisfy the given condition" in {
    val expectedResult = List(ConstraintFailure("1 rows did not satisfy constraint (column > 1)"))
    Check(makeIntegerDf(List(1, 2, 3))).satisfies(new Column("column") > 1).
      run().constraintResults shouldBe expectedResult
  }

  "A conditional satisfies check" should "succeed if all rows where the statement is true, satisfy the given condition" in {
    val expectedResult = List(ConstraintSuccess("Constraint (column1 = 1) -> (column2 = 0) is satisfied"))
    val condition = (new Column("column1") === 1) -> (new Column("column2") === 0)
    Check(makeIntegersDf(
      List(1, 0),
      List(2, 0),
      List(3, 0)
    )).satisfies(condition).run().constraintResults shouldBe expectedResult
  }

  it should "succeed if there are no rows where the statement is true" in {
    val expectedResult = List(ConstraintSuccess("Constraint (column1 = 5) -> (column2 = 100) is satisfied"))
    val condition = (new Column("column1") === 5) -> (new Column("column2") === 100)
    Check(makeIntegersDf(
      List(1, 0),
      List(1, 1),
      List(3, 0)
    )).satisfies(condition).run().constraintResults shouldBe expectedResult
  }

  it should "fail if there are rows that do not satisfy the given condition" in {
    val expectedResult = List(ConstraintFailure("1 rows did not satisfy constraint (column1 = 1) -> (column2 = 0)"))
    val condition = (new Column("column1") === 1) -> (new Column("column2") === 0)
    Check(makeIntegersDf(
      List(1, 0),
      List(1, 1),
      List(3, 0)
    )).satisfies(condition).run().constraintResults shouldBe expectedResult
  }

  "A unique key check" should "succeed if a given column defines a key" in {
    val expectedResult = List(ConstraintSuccess("Columns column1 are a key"))
    val df = makeIntegersDf(
      List(1,2),
      List(2,3),
      List(3,3)
    )
    Check(df).hasUniqueKey("column1").run().constraintResults shouldBe expectedResult
  }

  it should "succeed if the given columns define a key" in {
    val expectedResult = List(ConstraintSuccess("Columns column1,column2 are a key"))
    val df = makeIntegersDf(
      List(1,2,3),
      List(2,3,3),
      List(3,2,3)
    )
    Check(df).hasUniqueKey("column1", "column2").run().constraintResults shouldBe expectedResult
  }

  it should "fail if there are duplicate rows using the given column as a key" in {
    val expectedResult = List(ConstraintFailure("Columns column1 are not a key"))
    val df = makeIntegersDf(
      List(1,2),
      List(2,3),
      List(2,3)
    )
    Check(df).hasUniqueKey("column1").run().constraintResults shouldBe expectedResult
  }

  it should "fail if there are duplicate rows using the given columns as a key" in {
    val expectedResult = List(ConstraintFailure("Columns column1,column2 are not a key"))
    val df = makeIntegersDf(
      List(1,2,3),
      List(2,3,3),
      List(1,2,3)
    )
    Check(df).hasUniqueKey("column1", "column2").run().constraintResults shouldBe expectedResult
  }

  "An is-always-null check" should "succeed if the column is always null" in {
    val expectedResult = List(ConstraintSuccess("Column column is null"))
    Check(makeNullableStringDf(List(null, null, null))).isAlwaysNull("column").
      run().constraintResults shouldBe expectedResult
  }

  it should "fail if the column is not always null" in {
    val expectedResult = List(ConstraintFailure("Column column has 1 non-null rows although it should be null"))
    Check(makeNullableStringDf(List("a", null, null))).isAlwaysNull("column").
      run().constraintResults shouldBe expectedResult
  }

  "An is-never-null check" should "succeed if the column contains no null values" in {
    val expectedResult = List(ConstraintSuccess("Column column is not null"))
    Check(makeNullableStringDf(List("a", "b", "c"))).isNeverNull("column").
      run().constraintResults shouldBe expectedResult
  }

  it should "fail if the column contains null values" in {
    val expectedResult = List(ConstraintFailure("Column column has 1 null rows although it should not be null"))
    Check(makeNullableStringDf(List("a", "b", null))).isNeverNull("column").
    run().constraintResults shouldBe expectedResult
  }

  "A to Int conversion check" should "succeed if all elements can be converted to Int" in {
    val expectedResult = List(ConstraintSuccess("Column column can be converted to Int"))
    Check(makeNullableStringDf(List("1", "2", "3"))).isConvertibleToInt("column").
      run().constraintResults shouldBe expectedResult
  }

  it should "succeed if all elements can be converted to Int or are null" in {
    val expectedResult = List(ConstraintSuccess("Column column can be converted to Int"))
    Check(makeNullableStringDf(List("1", "2", null))).isConvertibleToInt("column").
      run().constraintResults shouldBe expectedResult
  }

  it should "fail if at least one element cannot be converted to Int" in {
    val expectedResult = List(ConstraintFailure("Column column contains 1 rows that cannot be converted to Int"))
    Check(makeNullableStringDf(List("1", "hallo", "3"))).isConvertibleToInt("column").
      run().constraintResults shouldBe expectedResult
  }

  "A to Double conversion check" should "succeed if all elements can be converted to Double" in {
    val expectedResult = List(ConstraintSuccess("Column column can be converted to Double"))
    Check(makeNullableStringDf(List("1.0", "2.0", "3"))).isConvertibleToDouble("column").
      run().constraintResults shouldBe expectedResult
  }

  it should "succeed if all elements can be converted to Double or are null" in {
    val expectedResult = List(ConstraintSuccess("Column column can be converted to Double"))
    Check(makeNullableStringDf(List("1", "2.0", null))).isConvertibleToDouble("column").
      run().constraintResults shouldBe expectedResult
  }

  it should "fail if at least one element cannot be converted to Double" in {
    val expectedResult = List(ConstraintFailure("Column column contains 1 rows that cannot be converted to Double"))
    Check(makeNullableStringDf(List("1", "hallo", "3"))).isConvertibleToDouble("column").
      run().constraintResults shouldBe expectedResult
  }

  "A to Long conversion check" should "succeed if all elements can be converted to Long" in {
    val expectedResult = List(ConstraintSuccess("Column column can be converted to Long"))
    Check(makeNullableStringDf(List("1", "2", "34565465756776"))).isConvertibleToLong("column").
      run().constraintResults shouldBe expectedResult
  }

  it should "succeed if all elements can be converted to Long or are null" in {
    val expectedResult = List(ConstraintSuccess("Column column can be converted to Long"))
    Check(makeNullableStringDf(List("1", "2", null))).isConvertibleToLong("column").
      run().constraintResults shouldBe expectedResult
  }

  it should "fail if at least one element cannot be converted to Long" in {
    val expectedResult = List(ConstraintFailure("Column column contains 1 rows that cannot be converted to Long"))
    Check(makeNullableStringDf(List("1", "hallo", "3"))).isConvertibleToLong("column").
      run().constraintResults shouldBe expectedResult
  }

  "A to Date conversion check" should "succeed if all elements can be converted to Date" in {
    val expectedResult = List(ConstraintSuccess("Column column can be converted to Date"))
    Check(makeNullableStringDf(List("2000-11-23 11:50:10", "2000-5-23 11:50:10", "2000-02-23 11:11:11")))
      .isConvertibleToDate("column", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).
      run().constraintResults shouldBe expectedResult
  }

  it should "succeed if all elements can be converted to Date or are null" in {
    val expectedResult = List(ConstraintSuccess("Column column can be converted to Date"))
    Check(makeNullableStringDf(List("2000-11-23 11:50:10", null, "2000-02-23 11:11:11")))
      .isConvertibleToDate("column", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).
      run().constraintResults shouldBe expectedResult
  }

  it should "fail if at least one element cannot be converted to Date" in {
    val expectedResult = List(ConstraintFailure("Column column contains 1 rows that cannot be converted to Date"))
    Check(makeNullableStringDf(List("2000-11-23 11:50:10", "abc", "2000-15-23 11:11:11")))
      .isConvertibleToDate("column", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).
      run().constraintResults shouldBe expectedResult
  }

  "A foreign key check" should "succeed if the given column is a foreign key pointing to the reference table" in {
    val expectedResult = List(ConstraintSuccess("Columns column->column define a foreign key"))
    val base = makeIntegerDf(List(1, 1, 1, 2, 2, 3))
    val ref = makeIntegerDf(List(1, 2, 3))
    Check(base).hasForeignKey(ref, "column" -> "column").run().constraintResults shouldBe expectedResult
  }

  it should "succeed if the given columns are a foreign key pointing to the reference table" in {
    val expectedResult = List(ConstraintSuccess("Columns column1->column1, column2->column2 define a foreign key"))
    val base = makeIntegersDf(List(1, 2, 3), List(1, 2, 5), List(1, 3, 3))
    val ref = makeIntegersDf(List(1, 2, 100), List(1, 3, 100))
    Check(base).hasForeignKey(ref, "column1" -> "column1", "column2" -> "column2").
      run().constraintResults shouldBe expectedResult
  }

  it should "succeed if the given columns are a foreign key pointing to the reference table having a different name" in {
    val expectedResult = List(ConstraintSuccess("Columns column1->column1, column3->column2 define a foreign key"))
    val base = makeIntegersDf(List(1, 2, 3), List(1, 2, 5), List(1, 3, 3))
    val ref = makeIntegersDf(List(1, 3, 100), List(1, 5, 100))
    Check(base).hasForeignKey(ref, "column1" -> "column1", "column3" -> "column2").
      run().constraintResults shouldBe expectedResult
  }

  it should "fail if the given column contains values that are not found in the reference table" in {
    val expectedResult = List(ConstraintFailure("Columns column->column do not define a foreign key (1 records do not match)"))
    val base = makeIntegerDf(List(1, 1, 1, 2, 2, 3))
    val ref = makeIntegerDf(List(1, 2))
    Check(base).hasForeignKey(ref, "column" -> "column").run().constraintResults shouldBe expectedResult
  }

  it should "fail if the given columns contains values that are not found in the reference table" in {
    val expectedResult = List(ConstraintFailure("Columns column1->column1, column2->column2 do not define a foreign key (1 records do not match)"))
    val base = makeIntegersDf(List(1, 2, 3), List(1, 2, 5), List(1, 5, 3))
    val ref = makeIntegersDf(List(1, 2, 100), List(1, 3, 100))
    Check(base).hasForeignKey(ref, "column1" -> "column1", "column2" -> "column2").
      run().constraintResults shouldBe expectedResult
  }

  it should "fail if the foreign key is not a primary key in the reference table" in {
    val expectedResult = List(ConstraintFailure("Columns column1, column2 are not a key in reference table"))
    val base = makeIntegersDf(List(1, 2, 3), List(1, 2, 5), List(1, 3, 3))
    val ref = makeIntegersDf(List(1, 3, 100), List(1, 5, 100), List(1, 5, 500))
    Check(base).hasForeignKey(ref, "column1" -> "column1", "column3" -> "column2").
      run().constraintResults shouldBe expectedResult
  }

  "A joinable check" should "succeed if a join on the given column yields at least one row" in {
    val expectedResult = List(ConstraintSuccess("Columns column->column can be used for joining (2 distinct rows match)"))
    val base = makeIntegerDf(List(1, 1, 1, 2, 2, 3))
    val ref = makeIntegerDf(List(1, 2, 5))
    Check(base).isJoinableWith(ref, "column" -> "column").run().constraintResults shouldBe expectedResult
  }

  it should "succeed if a join on the given columns yields at least one row" in {
    val expectedResult = List(ConstraintSuccess("Columns column1->column1, column2->column2 can be used for joining (1 distinct rows match)"))
    val base = makeIntegersDf(List(1, 2, 3), List(1, 2, 5), List(1, 3, 3))
    val ref = makeIntegersDf(List(1, 2, 100), List(1, 5, 100))
    Check(base).isJoinableWith(ref, "column1" -> "column1", "column2" -> "column2").
      run().constraintResults shouldBe expectedResult
  }

  it should "succeed if a join on the given columns yields at least one row if the columns have a different name" in {
    val expectedResult = List(ConstraintSuccess("Columns column1->column1, column3->column2 can be used for joining (1 distinct rows match)"))
    val base = makeIntegersDf(List(1, 2, 5), List(1, 2, 5), List(1, 100, 3))
    val ref = makeIntegersDf(List(1, 3, 100), List(1, 500, 100))
    Check(base).isJoinableWith(ref, "column1" -> "column1", "column3" -> "column2").
      run().constraintResults shouldBe expectedResult
  }

  it should "fail if a join on the given columns yields no result" in {
    val expectedResult = List(ConstraintFailure("Columns column1->column1, column3->column2 cannot be used for joining (no rows match)"))
    val base = makeIntegersDf(List(1, 2, 5), List(1, 2, 5), List(1, 100, 3))
    val ref = makeIntegersDf(List(1, 1, 100), List(1, 10, 100))
    Check(base).isJoinableWith(ref, "column1" -> "column1", "column3" -> "column2").
      run().constraintResults shouldBe expectedResult
  }

  it should "fail if a join on the given columns is not possible due to mismatching types" in {
    val expectedResult = List(ConstraintFailure("Columns column->column cannot be used for joining (no rows match)"))
    val base = makeNullableStringDf(List("a", "b"))
    val ref = makeIntegerDf(List(1, 2, 3))
    Check(base).isJoinableWith(ref, "column" -> "column").run().constraintResults shouldBe expectedResult
  }

  "A check if a column is in the given values" should "succeed if all values are inside" in {
    val expectedResult = List(ConstraintSuccess("Column column contains only values in Set(a, b, c, d)"))
    Check(makeNullableStringDf(List("a", "b", "c", "c"))).isAnyOf("column", Set("a", "b", "c", "d")).
      run().constraintResults shouldBe expectedResult
  }

  it should "succeed if all values are inside or null" in {
    val expectedResult = List(ConstraintSuccess("Column column contains only values in Set(a, b, c, d)"))
    Check(makeNullableStringDf(List("a", "b", "c", null))).isAnyOf("column", Set("a", "b", "c", "d")).
      run().constraintResults shouldBe expectedResult
  }

  it should "fail if there are values not inside" in {
    val expectedResult = List(ConstraintFailure("Column column contains 2 rows that are not in Set(a, b, d)"))
    Check(makeNullableStringDf(List("a", "b", "c", "c"))).isAnyOf("column", Set("a", "b", "d")).
      run().constraintResults shouldBe expectedResult
  }

  "A check if a column satisfies the given regex" should "succeed if all values satisfy the regex" in {
    val expectedResult = List(ConstraintSuccess("Column column matches ^Hello"))
    Check(makeNullableStringDf(List("Hello A", "Hello B", "Hello C"))).isMatchingRegex("column", "^Hello").
      run().constraintResults shouldBe expectedResult
  }

  it should "succeed if all values satisfy the regex or are null" in {
    val expectedResult = List(ConstraintSuccess("Column column matches ^Hello"))
    Check(makeNullableStringDf(List("Hello A", "Hello B", null))).isMatchingRegex("column", "^Hello").
      run().constraintResults shouldBe expectedResult
  }

  it should "fail if there are values not satisfying the regex" in {
    val expectedResult = List(ConstraintFailure("Column column contains 2 rows that do not match ^Hello A$"))
    Check(makeNullableStringDf(List("Hello A", "Hello B", "Hello C"))).isMatchingRegex("column", "^Hello A$").
      run().constraintResults shouldBe expectedResult
  }

  "Multiple checks" should "produce a list of constraint results in the same order" in {
    val expectedResult = List(
      ConstraintSuccess("The number of rows is equal to 3"),
      ConstraintFailure("The actual number of rows 3 is not equal to the expected 2"),
      ConstraintSuccess("Constraint column > 0 is satisfied")
    )
    Check(makeIntegerDf(List(1,2,3))).hasNumRowsEqualTo(3).hasNumRowsEqualTo(2).satisfies("column > 0").
      run().constraintResults shouldBe expectedResult
  }

  "It" should "be possible to specify a display name for a data frame" in {
    Check(makeIntegerDf(List(1,2,3)), displayName = Option("Integer Data Frame")).run
  }

  "A check from a SQLContext" should "load the given table" in {
    val expectedResult = List(ConstraintSuccess("The number of rows is equal to 3"))
    val df = makeIntegerDf(List(1,2,3), sql)
    val tableName = "myintegerdf1"
    df.registerTempTable(tableName)
    Check.sqlTable(sql, tableName).hasNumRowsEqualTo(3).run().constraintResults shouldBe expectedResult
  }

  it should "require the table to exist" in {
    intercept[IllegalArgumentException] {
      Check.sqlTable(sql, "doesnotexist").run
    }
  }

  "A check from a HiveContext" should "load the given table from the given database" in {
    val expectedResult = List(ConstraintSuccess("The number of rows is equal to 3"))
    val tableName = "myintegerdf2"
    val databaseName = "testDb"
    hive.sql(s"CREATE DATABASE $databaseName")
    hive.sql(s"USE $databaseName")
    val df = makeIntegerDf(List(1,2,3), hive)
    df.registerTempTable(tableName)
    hive.sql(s"USE default")
    Check.hiveTable(hive, databaseName, tableName).hasNumRowsEqualTo(3).run().constraintResults shouldBe expectedResult
    hive.sql(s"DROP DATABASE $databaseName")
  }

  it should "require the table to exist" in {
    intercept[IllegalArgumentException] {
      Check.hiveTable(hive, "default", "doesnotexist").run
    }
  }

  "A to boolean conversion check" should "succeed if column values are true and false only" in {
    val expectedResult = List(ConstraintSuccess("Column column can be converted to Boolean"))
    Check(makeNullableStringDf(List("true", "false"))).isConvertibleToBoolean("column").
      run().constraintResults shouldBe expectedResult
  }

  it should "fail if column values are not true and false only" in {
    val expectedResult = List(ConstraintFailure("Column column contains 1 rows that cannot be converted to Boolean"))
    Check(makeNullableStringDf(List("true", "false", "error"))).isConvertibleToBoolean("column").
      run().constraintResults shouldBe expectedResult
  }

  it should "succeed if column values are true/TRUE and false/FALSE if case sensitive is false" in {
    val expectedResult = List(ConstraintSuccess("Column column can be converted to Boolean"))
    val df = makeNullableStringDf(List("true", "false", "TRUE", "FALSE", "True", "fAlsE"))
    Check(df).isConvertibleToBoolean("column", isCaseSensitive = false).run().constraintResults shouldBe expectedResult
  }

  it should "fail if column values are true/TRUE and false/FALSE if case sensitive is true" in {
    val expectedResult = List(ConstraintFailure("Column column contains 4 rows that cannot be converted to Boolean"))
    val df = makeNullableStringDf(List("true", "false", "TRUE", "FALSE", "True", "fAlsE"))
    Check(df).isConvertibleToBoolean("column", isCaseSensitive = true).run().constraintResults shouldBe expectedResult
  }

  it should "succeed if column values are 1 and 0 only" in {
    val expectedResult = List(ConstraintSuccess("Column column can be converted to Boolean"))
    Check(makeNullableStringDf(List("1", "0"))).isConvertibleToBoolean("column", "1", "0").
      run().constraintResults shouldBe expectedResult
  }

  it should "fail if column values are not 1 and 0 only" in {
    val expectedResult = List(ConstraintFailure("Column column contains 1 rows that cannot be converted to Boolean"))
    Check(makeNullableStringDf(List("1", "0", "2"))).isConvertibleToBoolean("column", "1", "0").
      run().constraintResults shouldBe expectedResult
  }

  it should "succeed if column values are convertible or null" in {
    val expectedResult = List(ConstraintSuccess("Column column can be converted to Boolean"))
    Check(makeNullableStringDf(List("1", "0", null))).isConvertibleToBoolean("column", "1", "0").
      run().constraintResults shouldBe expectedResult
  }

}
