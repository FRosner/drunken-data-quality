package de.frosner.ddq.constraints

import de.frosner.ddq.core.Check
import de.frosner.ddq.testutils.{SparkContexts, TestData}
import org.apache.spark.sql.AnalysisException
import org.scalatest.{FlatSpec, Matchers}

class AlwaysNullConstraintTest extends FlatSpec with Matchers with SparkContexts {

  "An AlwaysNullConstraint" should "succeed if the column is always null" in {
    val column = "column"
    val check = Check(TestData.makeNullableStringDf(spark, List(null, null, null))).isAlwaysNull(column)
    val constraint = check.constraints.head
    val result = AlwaysNullConstraintResult(
      constraint = AlwaysNullConstraint(column),
      data = Some(AlwaysNullConstraintResultData(nonNullRows = 0L)),
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if the column is not always null" in {
    val column = "column"
    val check = Check(TestData.makeNullableStringDf(spark, List("a", null, null))).isAlwaysNull(column)
    val constraint = check.constraints.head
    val result = AlwaysNullConstraintResult(
      constraint = AlwaysNullConstraint(column),
      data = Some(AlwaysNullConstraintResultData(nonNullRows = 1L)),
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "error if the column is not existing" in {
    val column = "notExisting"
    val check = Check(TestData.makeNullableStringDf(spark, List("a", null, null))).isAlwaysNull(column)
    val constraint = check.constraints.head
    val result = check.run().constraintResults(constraint)
    result match {
      case AlwaysNullConstraintResult(
      AlwaysNullConstraint("notExisting"),
      constraintError: ConstraintError,
      None
      ) =>
        val analysisException = constraintError.throwable.asInstanceOf[AnalysisException]
        analysisException.message shouldBe "cannot resolve '`notExisting`' given input columns: [column]"
    }
  }

  "An AlwaysNullConstraintResult" should "have the correct success message" in {
    val constraint = AlwaysNullConstraint("c")
    val result = AlwaysNullConstraintResult(
      constraint = constraint,
      status = ConstraintSuccess,
      data = Some(AlwaysNullConstraintResultData(0L))
    )
    result.message shouldBe "Column c is always null."
  }

  it should "have the correct failure message (one row)" in {
    val constraint = AlwaysNullConstraint("c")
    val result = AlwaysNullConstraintResult(
      constraint = constraint,
      status = ConstraintFailure,
      data = Some(AlwaysNullConstraintResultData(1L))
    )
    result.message shouldBe "Column c contains 1 non-null row (should always be null)."
  }

  it should "have the correct failure message (multiple rows)" in {
    val constraint = AlwaysNullConstraint("c")
    val result = AlwaysNullConstraintResult(
      constraint = constraint,
      status = ConstraintFailure,
      data = Some(AlwaysNullConstraintResultData(2L))
    )
    result.message shouldBe "Column c contains 2 non-null rows (should always be null)."
  }

  it should "have the correct error message" in {
    val constraint = AlwaysNullConstraint("c")
    val result = AlwaysNullConstraintResult(
      constraint = constraint,
      status = ConstraintError(new IllegalArgumentException("column c not found")),
      data = None
    )
    result.message shouldBe "Checking column c for being always null failed: " +
      "java.lang.IllegalArgumentException: column c not found"
  }

  it should "throw an exception if it is created with an illegal combination of fields" in {
    intercept[IllegalConstraintResultException] {
      AlwaysNullConstraintResult(
        constraint = AlwaysNullConstraint("c"),
        status = ConstraintFailure,
        data = None
      )
    }
  }

}
