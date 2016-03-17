package de.frosner.ddq.constraints

import de.frosner.ddq.core.Check
import de.frosner.ddq.testutils.{TestData, SparkContexts}
import org.apache.spark.sql.DataFrame
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

class JoinableConstraintTest extends FlatSpec with Matchers with MockitoSugar with SparkContexts {

  "A JoinableConstraint" should "succeed if a join on the given column yields at least one row" in {
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

  "A JoinableConstraintResult" should "have the correct success message" in {
    val ref = mock[DataFrame]
    when(ref.toString).thenReturn("ref")
    val constraint = JoinableConstraint(Seq("c1" -> "c2"), ref)
    val result = JoinableConstraintResult(constraint, 1L, 1L, ConstraintSuccess)
    result.message shouldBe "Key c1->c2 can be used for joining. " +
      "Join columns cardinality in base table: 1. " +
      "Join columns cardinality after joining: 1 (100.00%)."
  }

  it should "compute the correct match percentage in the success message" in {
    val ref = mock[DataFrame]
    when(ref.toString).thenReturn("ref")
    val constraint = JoinableConstraint(Seq("c1" -> "c2"), ref)
    val result = JoinableConstraintResult(constraint, 2L, 1L, ConstraintSuccess)
    result.message shouldBe "Key c1->c2 can be used for joining. " +
      "Join columns cardinality in base table: 2. " +
      "Join columns cardinality after joining: 1 (50.00%)."
  }

  it should "have the correct failure message" in {
    val ref = mock[DataFrame]
    when(ref.toString).thenReturn("ref")
    val constraint = JoinableConstraint(Seq("c1" -> "c2", "c5" -> "c2"), ref)
    val result = JoinableConstraintResult(constraint, 5L, 0L, ConstraintFailure)
    result.message shouldBe "Key c1->c2, c5->c2 cannot be used for joining (no result)."
  }

}
