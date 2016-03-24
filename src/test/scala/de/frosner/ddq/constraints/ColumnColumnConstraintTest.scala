package de.frosner.ddq.constraints

import de.frosner.ddq.core.Check
import de.frosner.ddq.testutils.{SparkContexts, TestData}
import org.apache.spark.sql.{AnalysisException, Column}
import org.scalatest.{FlatSpec, Matchers}

class ColumnColumnConstraintTest extends FlatSpec with Matchers with SparkContexts {

  "A ColumnColumnConstraint" should "succeed if all rows satisfy the given condition" in {
    val constraintColumn = new Column("column") > 0
    val check = Check(TestData.makeIntegerDf(sql, List(1, 2, 3))).satisfies(constraintColumn)
    val constraint = check.constraints.head
    val result = ColumnColumnConstraintResult(
      constraint = ColumnColumnConstraint(constraintColumn),
      data = Some(ColumnColumnConstraintResultData(failedRows = 0L)),
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if there is a row that does not satisfy the given condition" in {
    val constraintColumn = new Column("column") > 1
    val check = Check(TestData.makeIntegerDf(sql, List(1, 2, 3))).satisfies(constraintColumn)
    val constraint = check.constraints.head
    val result = ColumnColumnConstraintResult(
      constraint = ColumnColumnConstraint(constraintColumn),
      data = Some(ColumnColumnConstraintResultData(failedRows = 1L)),
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "error if the condition references a non-existing column" in {
    val constraintColumn = new Column("notExisting") > 1
    val check = Check(TestData.makeIntegerDf(sql, List(1, 2, 3))).satisfies(constraintColumn)
    val constraint = check.constraints.head
    val result = check.run().constraintResults(constraint)
    result match {
      case ColumnColumnConstraintResult(
      ColumnColumnConstraint(column),
      None,
      constraintError: ConstraintError
      ) => {
        val analysisException = constraintError.throwable.asInstanceOf[AnalysisException]
        analysisException.message shouldBe "cannot resolve 'notExisting' given input columns column"
      }
    }
  }

  "A ColumnColumnConstraintResult" should "have the correct success message" in {
    val constraint = ColumnColumnConstraint(new Column("c") === 5)
    val result = ColumnColumnConstraintResult(
      constraint = constraint,
      data = Some(ColumnColumnConstraintResultData(failedRows = 0L)),
      status = ConstraintSuccess
    )
    result.message shouldBe "Constraint (c = 5) is satisfied."
  }

  it should "have the correct failure message (one row)" in {
    val constraint = ColumnColumnConstraint(new Column("c") === 5)
    val result = ColumnColumnConstraintResult(
      constraint = constraint,
      data = Some(ColumnColumnConstraintResultData(failedRows = 1L)),
      status = ConstraintFailure
    )
    result.message shouldBe "1 row did not satisfy constraint (c = 5)."
  }

  it should "have the correct failure message (multiple rows)" in {
    val constraint = ColumnColumnConstraint(new Column("c") === 5)
    val result = ColumnColumnConstraintResult(
      constraint = constraint,
      data = Some(ColumnColumnConstraintResultData(failedRows = 2L)),
      status = ConstraintFailure
    )
    result.message shouldBe "2 rows did not satisfy constraint (c = 5)."
  }

  it should "have the correct error message" in {
    val constraint = ColumnColumnConstraint(new Column("c") === 5)
    val result = ColumnColumnConstraintResult(
      constraint = constraint,
      data = None,
      status = ConstraintError(new IllegalArgumentException("column c not found"))
    )
    result.message shouldBe "Checking constraint (c = 5) failed: " +
      "java.lang.IllegalArgumentException: column c not found"
  }

  it should "throw an exception if it is created with an illegal combination of fields" in {
    intercept[IllegalConstraintResultException] {
      ColumnColumnConstraintResult(
        constraint = ColumnColumnConstraint(new Column("c") === 5),
        status = ConstraintFailure,
        data = None
      )
    }
  }

}
