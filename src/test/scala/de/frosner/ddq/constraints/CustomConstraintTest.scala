package de.frosner.ddq.constraints

import de.frosner.ddq.core.Check
import de.frosner.ddq.testutils.{SparkContexts, TestData}
import org.apache.spark.sql.AnalysisException
import org.scalatest.{FlatSpec, Matchers}

class CustomConstraintTest extends FlatSpec with Matchers with SparkContexts {

  "A CustomConstraint" should "succeed if the function returns a success message" in {
    val constraintName = "name"
    val successMsg = "success"
    val check = Check(TestData.makeNullableStringDf(spark, List("a"))).custom(constraintName, {
      df => Right(successMsg)
    })
    val constraint = check.constraints.head
    val result = CustomConstraintResult(
      constraint = constraint.asInstanceOf[CustomConstraint],
      message = s"Custom constraint '$constraintName' succeeded: $successMsg",
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if the function returns a failure message" in {
    val constraintName = "name"
    val failureMsg = "failure"
    val check = Check(TestData.makeNullableStringDf(spark, List("a"))).custom(constraintName, {
      df => Left(failureMsg)
    })
    val constraint = check.constraints.head
    val result = CustomConstraintResult(
      constraint = constraint.asInstanceOf[CustomConstraint],
      message = s"Custom constraint '$constraintName' failed: $failureMsg",
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "error if the function throws an exception" in {
    val constraintName = "name"
    val exception = new Exception()
    val check = Check(TestData.makeNullableStringDf(spark, List("a"))).custom(constraintName, {
      df => throw exception
    })
    val constraint = check.constraints.head
    val result = check.run().constraintResults(constraint)
    result match {
      case CustomConstraintResult(
      customConstraint: CustomConstraint,
      "Custom constraint 'name' errored: java.lang.Exception",
      constraintError: ConstraintError
      ) => {
        constraintError.throwable shouldBe exception
      }
    }
  }

}
