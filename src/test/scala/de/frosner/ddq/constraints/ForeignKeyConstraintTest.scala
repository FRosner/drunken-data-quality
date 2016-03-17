package de.frosner.ddq.constraints

import java.text.SimpleDateFormat

import de.frosner.ddq.core.Check
import de.frosner.ddq.testutils.{SparkContexts, TestData}
import org.apache.spark.sql.DataFrame
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

class ForeignKeyConstraintTest extends FlatSpec with Matchers with MockitoSugar with SparkContexts {

  "A ForeignKeyConstraint" should "succeed if the given column is a foreign key pointing to the reference table" in {
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

  "A ForeignKeyConstraintResult" should "have the correct success message (one column)" in {
    val ref = mock[DataFrame]
    when(ref.toString).thenReturn("ref")
    val constraint = ForeignKeyConstraint(Seq("a" -> "b"), ref)
    val result = ForeignKeyConstraintResult(constraint, Option(0L), ConstraintSuccess)
    result.message shouldBe "Column a->b defines a foreign key pointing to the reference table ref."
  }

  it should "have the correct success message (multiple columns)" in {
    val ref = mock[DataFrame]
    when(ref.toString).thenReturn("ref")
    val constraint = ForeignKeyConstraint(Seq("a" -> "b", "c" -> "d"), ref)
    val result = ForeignKeyConstraintResult(constraint, Option(0L), ConstraintSuccess)
    result.message shouldBe "Columns a->b, c->d define a foreign key pointing to the reference table ref."
  }

  it should "have the correct failure message if the columns are no key in the reference table (one column)" in {
    val ref = mock[DataFrame]
    when(ref.toString).thenReturn("ref")
    val constraint = ForeignKeyConstraint(Seq("a" -> "b"), ref)
    val result = ForeignKeyConstraintResult(constraint, None, ConstraintFailure)
    result.message shouldBe "Column a->b is not a key in the reference table."
  }

  it should "have the correct failure message if the columns are no key in the reference table (multiple columns)" in {
    val ref = mock[DataFrame]
    when(ref.toString).thenReturn("ref")
    val constraint = ForeignKeyConstraint(Seq("a" -> "b", "c" -> "d"), ref)
    val result = ForeignKeyConstraintResult(constraint, None, ConstraintFailure)
    result.message shouldBe "Columns a->b, c->d are not a key in the reference table."
  }

  it should "have the correct failure message if the columns don't define a foreign key (one column, one row)" in {
    val ref = mock[DataFrame]
    when(ref.toString).thenReturn("ref")
    val constraint = ForeignKeyConstraint(Seq("a" -> "b"), ref)
    val result = ForeignKeyConstraintResult(constraint, Some(1L), ConstraintFailure)
    result.message shouldBe "Column a->b does not define a foreign key pointing to ref. 1 row does not match."
  }

  it should "have the correct failure message if the columns don't define a foreign key (multiple columns, multiple rows)" in {
    val ref = mock[DataFrame]
    when(ref.toString).thenReturn("ref")
    val constraint = ForeignKeyConstraint(Seq("a" -> "b", "c" -> "d"), ref)
    val result = ForeignKeyConstraintResult(constraint, Some(2L), ConstraintFailure)
    result.message shouldBe "Columns a->b, c->d do not define a foreign key pointing to ref. 2 rows do not match."
  }

}
