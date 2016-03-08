package de.frosner.ddq.constraints

import java.text.SimpleDateFormat

import de.frosner.ddq.core.Check
import de.frosner.ddq.testutils.TestData
import org.apache.spark.sql.types.{LongType, DoubleType, StringType, IntegerType}
import org.apache.spark.sql.{Column, SQLContext}
import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers, FlatSpec}

class ConstraintTest extends FlatSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with MockitoSugar {

  private val sc = TestHive.sparkContext
  private val sql = new SQLContext(sc)

  "A unique key check" should "succeed if a given column defines a key" in {
    val df = TestData.makeIntegersDf(sql,
      List(1,2),
      List(2,3),
      List(3,3)
    )
    val column = "column1"

    val check = Check(df).hasUniqueKey(column)
    val constraint = check.constraints.head
    val result = UniqueKeyConstraintResult(
      constraint = UniqueKeyConstraint(Seq(column)),
      numNonUniqueTuples = 0L,
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if the given columns define a key" in {
    val df = TestData.makeIntegersDf(sql,
      List(1,2,3),
      List(2,3,3),
      List(3,2,3)
    )
    val column1 = "column1"
    val column2 = "column2"

    val check = Check(df).hasUniqueKey(column1, column2)
    val constraint = check.constraints.head
    val result = UniqueKeyConstraintResult(
      constraint = UniqueKeyConstraint(Seq(column1, column2)),
      numNonUniqueTuples = 0L,
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if there are duplicate rows using the given column as a key" in {
    val df = TestData.makeIntegersDf(sql,
      List(1,2),
      List(2,3),
      List(2,3)
    )
    val column = "column1"

    val check = Check(df).hasUniqueKey(column)
    val constraint = check.constraints.head
    val result = UniqueKeyConstraintResult(
      constraint = UniqueKeyConstraint(Seq(column)),
      numNonUniqueTuples = 1L,
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if there are duplicate rows using the given columns as a key" in {
    val df = TestData.makeIntegersDf(sql,
      List(1,2,3),
      List(2,3,3),
      List(1,2,3)
    )
    val column1 = "column1"
    val column2 = "column2"

    val check = Check(df).hasUniqueKey(column1, column2)
    val constraint = check.constraints.head
    val result = UniqueKeyConstraintResult(
      constraint = UniqueKeyConstraint(Seq(column1, column2)),
      numNonUniqueTuples = 1L,
      status = ConstraintFailure
    )

    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "A number of rows check" should "succeed if the actual number of rows is equal to the expected" in {
    val check = Check(TestData.makeIntegerDf(sql, List(1, 2, 3))).hasNumRows(_ === 3)
    val constraint = check.constraints.head
    val result = NumberOfRowsConstraintResult(
      constraint = NumberOfRowsConstraint(new Column(NumberOfRowsConstraint.countKey) === 3),
      actual = 3L,
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if the number of rows is not in the expected range" in {
    val check = Check(TestData.makeIntegerDf(sql, List(1, 2, 3))).hasNumRows(
      numRows => numRows < 3 || numRows > 3
    )
    val constraint = check.constraints.head
    val numRowsColumn = new Column(NumberOfRowsConstraint.countKey)
    val result = NumberOfRowsConstraintResult(
      constraint = NumberOfRowsConstraint(numRowsColumn < 3 || numRowsColumn > 3),
      actual = 3L,
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "A satisfies check (String)" should "succeed if all rows satisfy the given condition" in {
    val constraintString = "column > 0"
    val check = Check(TestData.makeIntegerDf(sql, List(1, 2, 3))).satisfies(constraintString)
    val constraint = check.constraints.head
    val result = StringColumnConstraintResult(
      constraint = StringColumnConstraint(constraintString),
      violatingRows = 0L,
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if there are rows that do not satisfy the given condition" in {
    val constraintString = "column > 1"
    val check = Check(TestData.makeIntegerDf(sql, List(1, 2, 3))).satisfies(constraintString)
    val constraint = check.constraints.head
    val result = StringColumnConstraintResult(
      constraint = StringColumnConstraint(constraintString),
      violatingRows = 1L,
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "A satisfies check (Column)" should "succeed if all rows satisfy the given condition" in {
    val constraintColumn = new Column("column") > 0
    val check = Check(TestData.makeIntegerDf(sql, List(1, 2, 3))).satisfies(constraintColumn)
    val constraint = check.constraints.head
    val result = ColumnColumnConstraintResult(
      constraint = ColumnColumnConstraint(constraintColumn),
      violatingRows = 0L,
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if there is a row that does not satisfy the given condition" in {
    val constraintColumn = new Column("column") > 1
    val check = Check(TestData.makeIntegerDf(sql, List(1, 2, 3))).satisfies(constraintColumn)
    val constraint = check.constraints.head
    val result = ColumnColumnConstraintResult(
      constraint = ColumnColumnConstraint(constraintColumn),
      violatingRows = 1L,
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "A conditional satisfies check" should "succeed if all rows where the statement is true, satisfy the given condition" in {
    val statement = new Column("column1") === 1
    val implication = new Column("column2") === 0
    val check = Check(TestData.makeIntegersDf(sql,
      List(1, 0),
      List(2, 0),
      List(3, 0)
    )).satisfies(statement -> implication)
    val constraint = check.constraints.head
    val result = ConditionalColumnConstraintResult(
      constraint = ConditionalColumnConstraint(statement, implication),
      violatingRows = 0L,
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if there are no rows where the statement is true" in {
    val statement = new Column("column1") === 5
    val implication = new Column("column2") === 100
    val check = Check(TestData.makeIntegersDf(sql,
      List(1, 0),
      List(1, 1),
      List(3, 0)
    )).satisfies(statement -> implication)
    val constraint = check.constraints.head
    val result = ConditionalColumnConstraintResult(
      constraint = ConditionalColumnConstraint(statement, implication),
      violatingRows = 0L,
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if there are rows that do not satisfy the given condition" in {
    val statement = new Column("column1") === 1
    val implication = new Column("column2") === 0
    val check = Check(TestData.makeIntegersDf(sql,
      List(1, 0),
      List(1, 1),
      List(3, 0)
    )).satisfies(statement -> implication)
    val constraint = check.constraints.head
    val result = ConditionalColumnConstraintResult(
      constraint = ConditionalColumnConstraint(statement, implication),
      violatingRows = 1L,
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "An is-always-null check" should "succeed if the column is always null" in {
    val column = "column"
    val check = Check(TestData.makeNullableStringDf(sql, List(null, null, null))).isAlwaysNull(column)
    val constraint = check.constraints.head
    val result = AlwaysNullConstraintResult(
      constraint = AlwaysNullConstraint(column),
      nonNullRows = 0L,
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if the column is not always null" in {
    val column = "column"
    val check = Check(TestData.makeNullableStringDf(sql, List("a", null, null))).isAlwaysNull(column)
    val constraint = check.constraints.head
    val result = AlwaysNullConstraintResult(
      constraint = AlwaysNullConstraint(column),
      nonNullRows = 1L,
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "An is-never-null check" should "succeed if the column contains no null values" in {
    val column = "column"
    val check = Check(TestData.makeNullableStringDf(sql, List("a", "b", "c"))).isNeverNull(column)
    val constraint = check.constraints.head
    val result = NeverNullConstraintResult(
      constraint = NeverNullConstraint(column),
      nullRows = 0L,
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if the column contains null values" in {
    val column = "column"
    val check = Check(TestData.makeNullableStringDf(sql, List("a", "b", null))).isNeverNull(column)
    val constraint = check.constraints.head
    val result = NeverNullConstraintResult(
      constraint = NeverNullConstraint(column),
      nullRows = 1L,
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "A conversion check" should "succeed if all elements can be converted" in {
    val column = "column"
    val targetType = IntegerType
    val check = Check(TestData.makeNullableStringDf(sql, List("1", "2", "3"))).isConvertibleTo(column, targetType)
    val constraint = check.constraints.head
    val result = TypeConversionConstraintResult(
      constraint = TypeConversionConstraint(column, targetType),
      originalType = StringType,
      failedRows = 0L,
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if all elements can be converted or are null" in {
    val column = "column"
    val targetType = DoubleType
    val check = Check(TestData.makeNullableStringDf(sql, List("1.0", "2.0", null))).isConvertibleTo(column, targetType)
    val constraint = check.constraints.head
    val result = TypeConversionConstraintResult(
      constraint = TypeConversionConstraint(column, targetType),
      originalType = StringType,
      failedRows = 0L,
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if at least one element cannot be converted" in {
    val column = "column"
    val targetType = LongType
    val check = Check(TestData.makeNullableStringDf(sql, List("1", "2", "hallo", "test"))).isConvertibleTo(column, targetType)
    val constraint = check.constraints.head
    val result = TypeConversionConstraintResult(
      constraint = TypeConversionConstraint(column, targetType),
      originalType = StringType,
      failedRows = 2L,
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "A to Date conversion check" should "succeed if all elements can be converted to Date" in {
    val column = "column"
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val check = Check(TestData.makeNullableStringDf(sql, List("2000-11-23 11:50:10", "2000-5-23 11:50:10", "2000-02-23 11:11:11"))).
      isFormattedAsDate(column, format)
    val constraint = check.constraints.head
    val result = DateFormatConstraintResult(
      constraint = DateFormatConstraint(column, format),
      failedRows = 0,
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if all elements can be converted to Date or are null" in {
    val column = "column"
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val check = Check(TestData.makeNullableStringDf(sql, List("2000-11-23 11:50:10", null, "2000-02-23 11:11:11"))).
      isFormattedAsDate(column, format)
    val constraint = check.constraints.head
    val result = DateFormatConstraintResult(
      constraint = DateFormatConstraint(column, format),
      failedRows = 0,
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if at least one element cannot be converted to Date" in {
    val column = "column"
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val check =  Check(TestData.makeNullableStringDf(sql, List("2000-11-23 11:50:10", "abc", "2000-15-23 11:11:11"))).
      isFormattedAsDate(column, format)
    val constraint = check.constraints.head
    val result = DateFormatConstraintResult(
      constraint = DateFormatConstraint(column, format),
      failedRows = 1,
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "A foreign key check" should "succeed if the given column is a foreign key pointing to the reference table" in {
    val columns = "column" -> "column"
    val base = TestData.makeIntegerDf(sql, List(1, 1, 1, 2, 2, 3))
    val ref = TestData.makeIntegerDf(sql, List(1, 2, 3))
    val check = Check(base).hasForeignKey(ref, columns)
    val constraint = check.constraints.head
    val result = ForeignKeyConstraintResult(
      constraint = ForeignKeyConstraint(Seq(columns), ref),
      numNonMatchingRefs = Some(0),
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if the given columns are a foreign key pointing to the reference table" in {
    val columns1 = "column1" -> "column1"
    val columns2 = "column2" -> "column2"
    val base = TestData.makeIntegersDf(sql, List(1, 2, 3), List(1, 2, 5), List(1, 3, 3))
    val ref = TestData.makeIntegersDf(sql, List(1, 2, 100), List(1, 3, 100))
    val check = Check(base).hasForeignKey(ref, columns1, columns2)
    val constraint = check.constraints.head
    val result = ForeignKeyConstraintResult(
      constraint = ForeignKeyConstraint(Seq(columns1, columns2), ref),
      numNonMatchingRefs = Some(0),
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if the given columns are a foreign key pointing to the reference table having a different name" in {
    val columns1 = "column1" -> "column1"
    val columns2 = "column3" -> "column2"
    val base = TestData.makeIntegersDf(sql, List(1, 2, 3), List(1, 2, 5), List(1, 3, 3))
    val ref = TestData.makeIntegersDf(sql, List(1, 3, 100), List(1, 5, 100))
    val check = Check(base).hasForeignKey(ref, columns1, columns2)
    val constraint = check.constraints.head
    val result = ForeignKeyConstraintResult(
      constraint = ForeignKeyConstraint(Seq(columns1, columns2), ref),
      numNonMatchingRefs = Some(0),
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if the given column contains values that are not found in the reference table" in {
    val columns = "column" -> "column"
    val base = TestData.makeIntegerDf(sql, List(1, 1, 1, 2, 2, 3))
    val ref = TestData.makeIntegerDf(sql, List(1, 2))
    val check = Check(base).hasForeignKey(ref, columns)
    val constraint = check.constraints.head
    val result = ForeignKeyConstraintResult(
      constraint = ForeignKeyConstraint(Seq(columns), ref),
      numNonMatchingRefs = Some(1),
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if the given columns contains values that are not found in the reference table" in {
    val columns1 = "column1" -> "column1"
    val columns2 = "column2" -> "column2"
    val base = TestData.makeIntegersDf(sql, List(1, 2, 3), List(1, 2, 5), List(1, 5, 3))
    val ref = TestData.makeIntegersDf(sql, List(1, 2, 100), List(1, 3, 100))
    val check = Check(base).hasForeignKey(ref, columns1, columns2)
    val constraint = check.constraints.head
    val result = ForeignKeyConstraintResult(
      constraint = ForeignKeyConstraint(Seq(columns1, columns2), ref),
      numNonMatchingRefs = Some(1),
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if the foreign key is not a primary key in the reference table" in {
    val columns1 = "column1" -> "column1"
    val columns2 = "column3" -> "column2"
    val base = TestData.makeIntegersDf(sql, List(1, 2, 3), List(1, 2, 5), List(1, 3, 3))
    val ref = TestData.makeIntegersDf(sql, List(1, 3, 100), List(1, 5, 100), List(1, 5, 500))
    val check = Check(base).hasForeignKey(ref, columns1, columns2)
    val constraint = check.constraints.head
    val result = ForeignKeyConstraintResult(
      constraint = ForeignKeyConstraint(Seq(columns1, columns2), ref),
      numNonMatchingRefs = None,
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "A joinable check" should "succeed if a join on the given column yields at least one row" in {
    val columns = "column" -> "column"
    val base = TestData.makeIntegerDf(sql, List(1, 1, 1, 2, 2, 3))
    val ref = TestData.makeIntegerDf(sql, List(1, 2, 5))
    val check = Check(base).isJoinableWith(ref, columns)
    val constraint = check.constraints.head
    val result = JoinableConstraintResult(
      constraint = JoinableConstraint(Seq(columns), ref),
      distinctBefore = 3L,
      matchingKeys = 2L,
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if a join on the given columns yields at least one row" in {
    val columns1 = "column1" -> "column1"
    val columns2 = "column2" -> "column2"
    val base = TestData.makeIntegersDf(sql, List(1, 2, 3), List(1, 2, 5), List(1, 3, 3))
    val ref = TestData.makeIntegersDf(sql, List(1, 2, 100), List(1, 5, 100))
    val check = Check(base).isJoinableWith(ref, columns1, columns2)
    val constraint = check.constraints.head
    val result = JoinableConstraintResult(
      constraint = JoinableConstraint(Seq(columns1, columns2), ref),
      distinctBefore = 2L,
      matchingKeys = 1L,
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if a join on the given columns yields at least one row if the columns have a different name" in {
    val columns1 = "column1" -> "column1"
    val columns2 = "column3" -> "column2"
    val base = TestData.makeIntegersDf(sql, List(1, 2, 5), List(1, 2, 5), List(1, 100, 3))
    val ref = TestData.makeIntegersDf(sql, List(1, 3, 100), List(1, 500, 100))
    val check = Check(base).isJoinableWith(ref, columns1, columns2)
    val constraint = check.constraints.head
    val result = JoinableConstraintResult(
      constraint = JoinableConstraint(Seq(columns1, columns2), ref),
      distinctBefore = 2L,
      matchingKeys = 1L,
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "compute the matched keys in a non-commutative way" in {
    val columns = "column" -> "column"

    val base = TestData.makeIntegerDf(sql, List(1, 1, 1, 1, 1, 1, 1, 1, 1, 2))
    val ref = TestData.makeIntegerDf(sql, List(1))

    val check1 = Check(base).isJoinableWith(ref, columns)
    val constraint1 = check1.constraints.head
    val result1 = JoinableConstraintResult(
      constraint = JoinableConstraint(Seq(columns), ref),
      distinctBefore = 2L,
      matchingKeys = 1L,
      status = ConstraintSuccess
    )
    check1.run().constraintResults shouldBe Map(constraint1 -> result1)

    val check2 = Check(ref).isJoinableWith(base, columns)
    val constraint2 = check2.constraints.head
    val result2 = JoinableConstraintResult(
      constraint = JoinableConstraint(Seq(columns), base),
      distinctBefore = 1L,
      matchingKeys = 1L,
      status = ConstraintSuccess
    )
    check2.run().constraintResults shouldBe Map(constraint2 -> result2)
  }

  it should "fail if a join on the given columns yields no result" in {
    val columns1 = "column1" -> "column1"
    val columns2 = "column3" -> "column2"
    val base = TestData.makeIntegersDf(sql, List(1, 2, 5), List(1, 2, 5), List(1, 100, 3))
    val ref = TestData.makeIntegersDf(sql, List(1, 1, 100), List(1, 10, 100))
    val check = Check(base).isJoinableWith(ref, columns1, columns2)
    val constraint = check.constraints.head
    val result = JoinableConstraintResult(
      constraint = JoinableConstraint(Seq(columns1, columns2), ref),
      distinctBefore = 2L,
      matchingKeys = 0L,
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if a join on the given columns is not possible due to mismatching types" in {
    val columns = "column" -> "column"
    val base = TestData.makeNullableStringDf(sql, List("a", "b"))
    val ref = TestData.makeIntegerDf(sql, List(1, 2, 3))
    val check = Check(base).isJoinableWith(ref, columns)
    val constraint = check.constraints.head
    val result = JoinableConstraintResult(
      constraint = JoinableConstraint(Seq(columns), ref),
      distinctBefore = 2L,
      matchingKeys = 0L,
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "A check if a column is in the given values" should "succeed if all values are inside" in {
    val column = "column"
    val allowed = Set[Any]("a", "b", "c", "d")
    val check = Check(TestData.makeNullableStringDf(sql, List("a", "b", "c", "c"))).isAnyOf(column, allowed)
    val constraint = check.constraints.head
    val result = AnyOfConstraintResult(
      constraint = AnyOfConstraint("column", allowed),
      failedRows = 0,
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if all values are inside or null" in {
    val column = "column"
    val allowed = Set[Any]("a", "b", "c", "d")
    val check = Check(TestData.makeNullableStringDf(sql, List("a", "b", "c", null))).isAnyOf(column, allowed)
    val constraint = check.constraints.head
    val result = AnyOfConstraintResult(
      constraint = AnyOfConstraint("column", allowed),
      failedRows = 0,
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if there are values not inside" in {
    val column = "column"
    val allowed = Set[Any]("a", "b", "d")
    val check = Check(TestData.makeNullableStringDf(sql, List("a", "b", "c", "c"))).isAnyOf(column, allowed)
    val constraint = check.constraints.head
    val result = AnyOfConstraintResult(
      constraint = AnyOfConstraint(column, allowed),
      failedRows = 2,
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "A check if a column satisfies the given regex" should "succeed if all values satisfy the regex" in {
    val column = "column"
    val regex = "^Hello"
    val check = Check(TestData.makeNullableStringDf(sql, List("Hello A", "Hello B", "Hello C"))).isMatchingRegex(column, regex)
    val constraint = check.constraints.head
    val result = RegexConstraintResult(
      constraint = RegexConstraint(column, regex),
      failedRows = 0L,
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if all values satisfy the regex or are null" in {
    val column = "column"
    val regex = "^Hello"
    val check = Check(TestData.makeNullableStringDf(sql, List("Hello A", "Hello B", null))).isMatchingRegex(column, regex)
    val constraint = check.constraints.head
    val result = RegexConstraintResult(
      constraint = RegexConstraint(column, regex),
      failedRows = 0L,
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if there is a row not satisfying the regex" in {
    val column = "column"
    val regex = "^Hello A$"
    val check = Check(TestData.makeNullableStringDf(sql, List("Hello A", "Hello A", "Hello B"))).isMatchingRegex(column, regex)
    val constraint = check.constraints.head
    val result = RegexConstraintResult(
      constraint = RegexConstraint(column, regex),
      failedRows = 1L,
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "A functional dependency check" should "succeed if the column values in the determinant set always correspond to " +
    "the column values in the dependent set" in {
    val determinantSet = Seq("column1", "column2")
    val dependentSet = Seq("column3")
    val check = Check(TestData.makeIntegersDf(sql,
      List(1, 2, 1, 1),
      List(9, 9, 9, 2),
      List(9, 9, 9, 3),
      List(3, 4, 3, 4),
      List(7, 7, 7, 5)
    )).hasFunctionalDependency(determinantSet, dependentSet)
    val constraint = check.constraints.head
    val result = FunctionalDependencyConstraintResult(
      constraint = FunctionalDependencyConstraint(determinantSet, dependentSet),
      failedRows = 0L,
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed also for dependencies where the determinant and dependent sets are not distinct" in {
    val determinantSet = Seq("column1", "column2")
    val dependentSet = Seq("column2", "column3")
    val check = Check(TestData.makeIntegersDf(sql, 
      List(1, 2, 3, 1),
      List(9, 9, 9, 2),
      List(9, 9, 9, 3),
      List(3, 4, 3, 4),
      List(7, 7, 7, 5)
    )).hasFunctionalDependency(determinantSet, dependentSet)
    val constraint = check.constraints.head
    val result = FunctionalDependencyConstraintResult(
      constraint = FunctionalDependencyConstraint(determinantSet, dependentSet),
      failedRows = 0L,
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if the determinant and dependent sets are equal" in {
    val determinantSet = Seq("column")
    val dependentSet = determinantSet
    val check = Check(TestData.makeIntegerDf(sql,
      List(1, 2, 3)
    )).hasFunctionalDependency(determinantSet, dependentSet)
    val constraint = check.constraints.head
    val result = FunctionalDependencyConstraintResult(
      constraint = FunctionalDependencyConstraint(determinantSet, dependentSet),
      failedRows = 0L,
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if the column values in the determinant set don't always correspond to the column values in the dependent set" in {
    val determinantSet = Seq("column1", "column2")
    val dependentSet = Seq("column3")
    val check = Check(TestData.makeIntegersDf(sql, 
      List(1, 2, 1, 1),
      List(9, 9, 9, 1),
      List(9, 9, 8, 1),
      List(3, 4, 3, 1),
      List(7, 7, 7, 1),
      List(7, 7, 6, 1)
    )).hasFunctionalDependency(determinantSet, dependentSet)
    val constraint = check.constraints.head
    val result = FunctionalDependencyConstraintResult(
      constraint = FunctionalDependencyConstraint(determinantSet, dependentSet),
      failedRows = 2L,
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail also for dependencies where the determinant and dependent sets are not distinct" in {
    val determinantSet = Seq("column1", "column2")
    val dependentSet = Seq("column2", "column3")
    val check = Check(TestData.makeIntegersDf(sql, 
      List(1, 2, 1, 1),
      List(9, 9, 9, 1),
      List(9, 9, 8, 1),
      List(3, 4, 3, 1),
      List(7, 7, 7, 1)
    )).hasFunctionalDependency(determinantSet, dependentSet)
    val constraint = check.constraints.head
    val result = FunctionalDependencyConstraintResult(
      constraint = FunctionalDependencyConstraint(determinantSet, dependentSet),
      failedRows = 1L,
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "require the determinant set to be non-empty" in {
    intercept[IllegalArgumentException] {
      Check(TestData.makeIntegersDf(sql, 
        List(1, 2, 3),
        List(4, 5, 6)
      )).hasFunctionalDependency(Seq.empty, Seq("column0"))
    }
  }

  it should "require the dependent set to be non-empty" in {
    intercept[IllegalArgumentException] {
      Check(TestData.makeIntegersDf(sql, 
        List(1, 2, 3),
        List(4, 5, 6)
      )).hasFunctionalDependency(Seq("column0"), Seq.empty)
    }
  }


}
