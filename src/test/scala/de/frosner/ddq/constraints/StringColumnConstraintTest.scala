package de.frosner.ddq.constraints

import de.frosner.ddq.core.Check
import de.frosner.ddq.testutils.{SparkContexts, TestData}
import org.apache.spark.sql.AnalysisException
import org.scalatest.{FlatSpec, Matchers}

class StringColumnConstraintTest extends FlatSpec with Matchers with SparkContexts {

  "A StringColumnConstraint" should "succeed if all rows satisfy the given condition" in {
    val constraintString = "column > 0"
    val check = Check(TestData.makeIntegerDf(sql, List(1, 2, 3))).satisfies(constraintString)
    val constraint = check.constraints.head
    val result = StringColumnConstraintResult(
      constraint = StringColumnConstraint(constraintString),
      data = Some(StringColumnConstraintResultData(failedRows = 0L)),
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
      data = Some(StringColumnConstraintResultData(failedRows = 1L)),
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "error if the column does not exist" in {
    val constraintString = "notExisting > 0"
    val check = Check(TestData.makeIntegerDf(sql, List(1, 2, 3))).satisfies(constraintString)
    val constraint = check.constraints.head
    val result = check.run().constraintResults(constraint)
    result match {
      case StringColumnConstraintResult(
      StringColumnConstraint("notExisting > 0"),
      None,
      constraintError: ConstraintError
      ) => {
        val analysisException = constraintError.throwable.asInstanceOf[AnalysisException]
        analysisException.message shouldBe "cannot resolve 'notExisting' given input columns column"
      }
    }
  }

  "A StringColumnConstraintResult" should "have the correct success message" in {
    val constraint = StringColumnConstraint("column > 0")
    val result = StringColumnConstraintResult(
      constraint = constraint,
      data = Some(StringColumnConstraintResultData(failedRows = 0L)),
      status = ConstraintSuccess
    )
    result.message shouldBe "Constraint column > 0 is satisfied."
  }

  it should "have the correct failure message (one row)" in {
    val constraint = StringColumnConstraint("column > 0")
    val result = StringColumnConstraintResult(
      constraint = constraint,
      data = Some(StringColumnConstraintResultData(failedRows = 1L)),
      status = ConstraintFailure
    )
    result.message shouldBe "1 row did not satisfy constraint column > 0."
  }

  it should "have the correct failure message (multiple rows)" in {
    val constraint = StringColumnConstraint("column > 0")
    val result = StringColumnConstraintResult(
      constraint = constraint,
      data = Some(StringColumnConstraintResultData(failedRows = 2L)),
      status = ConstraintFailure
    )
    result.message shouldBe "2 rows did not satisfy constraint column > 0."
  }

  it should "have the correct error message" in {
    val constraint = StringColumnConstraint("column > 0")
    val result = StringColumnConstraintResult(
      constraint = constraint,
      data = None,
      status = ConstraintError(new IllegalArgumentException("error"))
    )
    result.message shouldBe "Checking constraint column > 0 failed: java.lang.IllegalArgumentException: error"
  }

  it should "throw an exception if it is created with an illegal combination of fields" in {
    intercept[IllegalConstraintResultException] {
      StringColumnConstraintResult(
        constraint = StringColumnConstraint("column > 0"),
        status = ConstraintFailure,
        data = None
      )
    }
  }

}
