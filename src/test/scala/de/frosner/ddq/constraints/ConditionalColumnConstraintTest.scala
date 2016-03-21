package de.frosner.ddq.constraints

import de.frosner.ddq.core.Check
import de.frosner.ddq.testutils.{SparkContexts, TestData}
import org.apache.spark.sql.{AnalysisException, Column}
import org.scalatest.{FlatSpec, Matchers}

class ConditionalColumnConstraintTest extends FlatSpec with Matchers with SparkContexts {

  "A ConditionalColumnConstraint" should "succeed if all rows where the statement is true, satisfy the given condition" in {
    val statement = new Column("column1") === 1
    val implication = new Column("column2") === 0
    val check = Check(TestData.makeIntegersDf(sql,
      List(1, 0),
      List(2, 0),
      List(3, 0)
    )).satisfies(statement -> implication)
    val constraint = check.constraints.head
    val result = ConditionalColumnConstraintResult(
      constraint = ConditionalColumnConstraint(statement, implication),
      data = Some(ConditionalColumnConstraintResultData(failedRows = 0L)),
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if there are no rows where the statement is true" in {
    val statement = new Column("column1") === 5
    val implication = new Column("column2") === 100
    val check = Check(TestData.makeIntegersDf(sql,
      List(1, 0),
      List(1, 1),
      List(3, 0)
    )).satisfies(statement -> implication)
    val constraint = check.constraints.head
    val result = ConditionalColumnConstraintResult(
      constraint = ConditionalColumnConstraint(statement, implication),
      data = Some(ConditionalColumnConstraintResultData(failedRows = 0L)),
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if there are rows that do not satisfy the given condition" in {
    val statement = new Column("column1") === 1
    val implication = new Column("column2") === 0
    val check = Check(TestData.makeIntegersDf(sql,
      List(1, 0),
      List(1, 1),
      List(3, 0)
    )).satisfies(statement -> implication)
    val constraint = check.constraints.head
    val result = ConditionalColumnConstraintResult(
      constraint = ConditionalColumnConstraint(statement, implication),
      data = Some(ConditionalColumnConstraintResultData(failedRows = 1L)),
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "error if the condition references a non-existing column" in {
    val statement = new Column("notExisting") === 1
    val implication = new Column("column2") === 0
    val check = Check(TestData.makeIntegersDf(sql,
      List(1, 0),
      List(1, 1),
      List(3, 0)
    )).satisfies(statement -> implication)
    val constraint = check.constraints.head
    val result = check.run().constraintResults(constraint)
    result match {
      case ConditionalColumnConstraintResult(
        ConditionalColumnConstraint(s, i),
        None,
        constraintError: ConstraintError
      ) => {
        val analysisException = constraintError.throwable.asInstanceOf[AnalysisException]
        analysisException.message shouldBe "cannot resolve 'notExisting' given input columns column1, column2"
      }
    }
  }

  "A ConditionalColumnConstraintResult" should "have the correct success message" in {
    val constraint = ConditionalColumnConstraint(new Column("c") === 5, new Column("d") === 2)
    val result = ConditionalColumnConstraintResult(
      constraint = constraint,
      data = Some(ConditionalColumnConstraintResultData(failedRows = 0L)),
      status = ConstraintSuccess
    )
    result.message shouldBe "Constraint (c = 5) -> (d = 2) is satisfied."
  }

  it should "have the correct failure message (one row)" in {
    val constraint = ConditionalColumnConstraint(new Column("c") === 5, new Column("d") === 2)
    val result = ConditionalColumnConstraintResult(
      constraint = constraint,
      data = Some(ConditionalColumnConstraintResultData(failedRows = 1L)),
      status = ConstraintFailure
    )
    result.message shouldBe "1 row did not satisfy constraint (c = 5) -> (d = 2)."
  }

  it should "have the correct failure message (multiple rows)" in {
    val constraint = ConditionalColumnConstraint(new Column("c") === 5, new Column("d") === 2)
    val result = ConditionalColumnConstraintResult(
      constraint = constraint,
      data = Some(ConditionalColumnConstraintResultData(failedRows = 2L)),
      status = ConstraintFailure
    )
    result.message shouldBe "2 rows did not satisfy constraint (c = 5) -> (d = 2)."
  }

  it should "have the correct error message" in {
    val constraint = ConditionalColumnConstraint(new Column("c") === 5, new Column("d") === 2)
    val result = ConditionalColumnConstraintResult(
      constraint = constraint,
      data = None,
      status = ConstraintError(new IllegalArgumentException("column c not found"))
    )
    result.message shouldBe "Checking constraint (c = 5) -> (d = 2) failed: " +
      "java.lang.IllegalArgumentException: column c not found"
  }

}
