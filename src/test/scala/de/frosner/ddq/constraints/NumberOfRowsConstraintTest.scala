package de.frosner.ddq.constraints

import de.frosner.ddq.core.Check
import de.frosner.ddq.testutils.{SparkContexts, TestData}
import org.apache.spark.sql.Column
import org.scalatest.{FlatSpec, Matchers}

class NumberOfRowsConstraintTest extends FlatSpec with Matchers with SparkContexts {

  "A NumberOfRowsConstraint" should "succeed if the actual number of rows is equal to the expected" in {
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

  "A NumberOfRowsConstraintResult" should "have the correct success message" in {
    val constraint = NumberOfRowsConstraint(new Column("count") > 5L)
    val result = NumberOfRowsConstraintResult(constraint, 5L, ConstraintSuccess)
    result.message shouldBe "The number of rows satisfies (count > 5)."
  }

  it should "have the correct failure message" in {
    val constraint = NumberOfRowsConstraint(new Column("count") === 5L)
    val result = NumberOfRowsConstraintResult(constraint, 4L, ConstraintFailure)
    result.message shouldBe "The actual number of rows 4 does not satisfy (count = 5)."
  }

}
