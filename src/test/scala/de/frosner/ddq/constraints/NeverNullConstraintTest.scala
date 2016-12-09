package de.frosner.ddq.constraints

import de.frosner.ddq.core.Check
import de.frosner.ddq.testutils.{SparkContexts, TestData}
import org.apache.spark.sql.AnalysisException
import org.scalatest.{FlatSpec, Matchers}

class NeverNullConstraintTest extends FlatSpec with Matchers with SparkContexts {

  "A NeverNullConstraint" should "succeed if the column contains no null values" in {
    val column = "column"
    val check = Check(TestData.makeNullableStringDf(spark, List("a", "b", "c"))).isNeverNull(column)
    val constraint = check.constraints.head
    val result = NeverNullConstraintResult(
      constraint = NeverNullConstraint(column),
      data = Some(NeverNullConstraintResultData(0L)),
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if the column contains null values" in {
    val column = "column"
    val check = Check(TestData.makeNullableStringDf(spark, List("a", "b", null))).isNeverNull(column)
    val constraint = check.constraints.head
    val result = NeverNullConstraintResult(
      constraint = NeverNullConstraint(column),
      data = Some(NeverNullConstraintResultData(1L)),
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "error if the column is not existing" in {
    val column = "notExisting"
    val check = Check(TestData.makeNullableStringDf(spark, List("a", null, null))).isNeverNull(column)
    val constraint = check.constraints.head
    val result = check.run().constraintResults(constraint)
    result match {
      case NeverNullConstraintResult(
      NeverNullConstraint("notExisting"),
      None,
      constraintError: ConstraintError
      ) => {
        val analysisException = constraintError.throwable.asInstanceOf[AnalysisException]
        analysisException.message shouldBe "cannot resolve '`notExisting`' given input columns: [column]"
      }
    }
  }

  "A NeverNullConstraintResult" should "have the correct success message" in {
    val constraint = NeverNullConstraint("c")
    val result = NeverNullConstraintResult(
      constraint = constraint,
      data = Some(NeverNullConstraintResultData(0L)),
      status = ConstraintSuccess
    )
    result.message shouldBe "Column c is never null."
  }

  it should "have the correct failure message (one row)" in {
    val constraint = NeverNullConstraint("c")
    val result = NeverNullConstraintResult(
      constraint = constraint,
      data = Some(NeverNullConstraintResultData(1L)),
      status = ConstraintFailure
    )
    result.message shouldBe "Column c contains 1 row that is null (should never be null)."
  }

  it should "have the correct failure message (multiple rows)" in {
    val constraint = NeverNullConstraint("c")
    val result = NeverNullConstraintResult(
      constraint = constraint,
      data = Some(NeverNullConstraintResultData(2L)),
      status = ConstraintFailure
    )
    result.message shouldBe "Column c contains 2 rows that are null (should never be null)."
  }

  it should "have the correct error message" in {
    val constraint = NeverNullConstraint("c")
    val result = NeverNullConstraintResult(
      constraint = constraint,
      status = ConstraintError(new IllegalArgumentException("column c not found")),
      data = None
    )
    result.message shouldBe "Checking column c for being never null failed: " +
      "java.lang.IllegalArgumentException: column c not found"
  }

  it should "throw an exception if it is created with an illegal combination of fields" in {
    intercept[IllegalConstraintResultException] {
      NeverNullConstraintResult(
        constraint = NeverNullConstraint("c"),
        status = ConstraintFailure,
        data = None
      )
    }
  }

}
