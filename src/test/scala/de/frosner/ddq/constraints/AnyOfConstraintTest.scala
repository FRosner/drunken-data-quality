package de.frosner.ddq.constraints

import de.frosner.ddq.core.Check
import de.frosner.ddq.testutils.{SparkContexts, TestData}
import org.apache.spark.sql.AnalysisException
import org.scalatest.{FlatSpec, Matchers}

class AnyOfConstraintTest extends FlatSpec with Matchers with SparkContexts {

  "An AnyOfConstraint" should "succeed if all values are inside" in {
    val column = "column"
    val allowed = Set[Any]("a", "b", "c", "d")
    val check = Check(TestData.makeNullableStringDf(spark, List("a", "b", "c", "c"))).isAnyOf(column, allowed)
    val constraint = check.constraints.head
    val result = AnyOfConstraintResult(
      constraint = AnyOfConstraint("column", allowed),
      data = Some(AnyOfConstraintResultData(failedRows = 0L)),
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if all values are inside or null" in {
    val column = "column"
    val allowed = Set[Any]("a", "b", "c", "d")
    val check = Check(TestData.makeNullableStringDf(spark, List("a", "b", "c", null))).isAnyOf(column, allowed)
    val constraint = check.constraints.head
    val result = AnyOfConstraintResult(
      constraint = AnyOfConstraint("column", allowed),
      data = Some(AnyOfConstraintResultData(failedRows = 0)),
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if there are values not inside" in {
    val column = "column"
    val allowed = Set[Any]("a", "b", "d")
    val check = Check(TestData.makeNullableStringDf(spark, List("a", "b", "c", "c"))).isAnyOf(column, allowed)
    val constraint = check.constraints.head
    val result = AnyOfConstraintResult(
      constraint = AnyOfConstraint(column, allowed),
      data = Some(AnyOfConstraintResultData(failedRows = 2L)),
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "error if there are values not inside" in {
    val allowed = Set[Any]("a", "b", "d")
    val check = Check(TestData.makeNullableStringDf(spark, List("a", "b", "c", "c"))).isAnyOf("notExisting", allowed)
    val constraint = check.constraints.head
    val result = check.run().constraintResults(constraint)
    result match {
      case AnyOfConstraintResult(
      AnyOfConstraint("notExisting", _),
      None,
      constraintError: ConstraintError
      ) =>
        val analysisException = constraintError.throwable.asInstanceOf[AnalysisException]
        analysisException.message shouldBe "cannot resolve '`notExisting`' given input columns: [column]"
    }
  }

  "An AnyOfConstraintResult" should "have the correct success message" in {
    val constraint = AnyOfConstraint("c", Set("a", "b"))
    val result = AnyOfConstraintResult(
      constraint = constraint,
      data = Some(AnyOfConstraintResultData(failedRows = 0L)),
      status = ConstraintSuccess
    )
    result.message shouldBe "Column c contains only values in Set(a, b)."
  }

  it should "have the correct failure message (one row)" in {
    val constraint = AnyOfConstraint("c", Set("a", "b"))
    val result = AnyOfConstraintResult(
      constraint = constraint,
      data = Some(AnyOfConstraintResultData(failedRows = 1L)),
      status = ConstraintFailure
    )
    result.message shouldBe "Column c contains 1 row that is not in Set(a, b)."
  }

  it should "have the correct failure message (multiple rows)" in {
    val constraint = AnyOfConstraint("c", Set("a", "b"))
    val result = AnyOfConstraintResult(
      constraint = constraint,
      data = Some(AnyOfConstraintResultData(failedRows = 2L)),
      status = ConstraintFailure
    )
    result.message shouldBe "Column c contains 2 rows that are not in Set(a, b)."
  }

  it should "have the correct error message" in {
    val constraint = AnyOfConstraint("c", Set("a", "b"))
    val result = AnyOfConstraintResult(
      constraint = constraint,
      data = None,
      status = ConstraintError(new IllegalArgumentException("reason"))
    )
    result.message shouldBe "Checking whether column c contains only values in Set(a, b) failed: " +
      "java.lang.IllegalArgumentException: reason"
  }

  it should "throw an exception if it is created with an illegal combination of fields" in {
    intercept[IllegalConstraintResultException] {
      AnyOfConstraintResult(
        constraint = AnyOfConstraint("c", Set("a", "b")),
        status = ConstraintFailure,
        data = None
      )
    }
  }

}
