package de.frosner.ddq.constraints

import de.frosner.ddq.core.Check
import de.frosner.ddq.testutils.{TestData, SparkContexts}
import org.scalatest.{FlatSpec, Matchers}

class UniqueKeyConstraintTest extends FlatSpec with Matchers with SparkContexts {

  "A UniqueKeyConstraint" should "succeed if a given column defines a key" in {
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

  "A UniqueKeyConstraintResult" should "have the correct success message (single column)" in {
    val constraint = UniqueKeyConstraint(Seq("column1"))
    val result = UniqueKeyConstraintResult(constraint, 0L, ConstraintSuccess)
    result.message shouldBe "Column column1 is a key."
  }

  it should "have the correct success message (multiple columns)" in {
    val constraint = UniqueKeyConstraint(Seq("column1", "column2"))
    val result = UniqueKeyConstraintResult(constraint, 0L, ConstraintSuccess)
    result.message shouldBe "Columns column1, column2 are a key."
  }

  it should "have the correct failure message (one column, one row)" in {
    val constraint = UniqueKeyConstraint(Seq("column1"))
    val result = UniqueKeyConstraintResult(constraint, 1L, ConstraintFailure)
    result.message shouldBe "Column column1 is not a key (1 non-unique tuple)."
  }

  it should "have the correct failure message (multiple columns, multiple rows)" in {
    val constraint = UniqueKeyConstraint(Seq("column1", "column2"))
    val result = UniqueKeyConstraintResult(constraint, 2L, ConstraintFailure)
    result.message shouldBe "Columns column1, column2 are not a key (2 non-unique tuples)."
  }

}
