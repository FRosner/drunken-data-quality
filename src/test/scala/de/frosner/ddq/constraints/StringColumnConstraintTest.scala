package de.frosner.ddq.constraints

import de.frosner.ddq.core.Check
import de.frosner.ddq.testutils.{TestData, SparkContexts}
import org.scalatest.{FlatSpec, Matchers}

class StringColumnConstraintTest extends FlatSpec with Matchers with SparkContexts {

  "A StringColumnConstraint" should "succeed if all rows satisfy the given condition" in {
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

  "A StringColumnConstraintResult" should "have the correct success message" in {
    val constraint = StringColumnConstraint("column > 0")
    val result = StringColumnConstraintResult(constraint, 0L, ConstraintSuccess)
    result.message shouldBe "Constraint column > 0 is satisfied."
  }

  it should "have the correct failure message (one row)" in {
    val constraint = StringColumnConstraint("column > 0")
    val result = StringColumnConstraintResult(constraint, 1L, ConstraintFailure)
    result.message shouldBe "1 row did not satisfy constraint column > 0."
  }

  it should "have the correct failure message (multiple rows)" in {
    val constraint = StringColumnConstraint("column > 0")
    val result = StringColumnConstraintResult(constraint, 2L, ConstraintFailure)
    result.message shouldBe "2 rows did not satisfy constraint column > 0."
  }

}
