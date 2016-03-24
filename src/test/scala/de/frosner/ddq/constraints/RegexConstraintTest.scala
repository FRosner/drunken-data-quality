package de.frosner.ddq.constraints

import de.frosner.ddq.core.Check
import de.frosner.ddq.testutils.{SparkContexts, TestData}
import org.apache.spark.sql.AnalysisException
import org.scalatest.{FlatSpec, Matchers}

class RegexConstraintTest extends FlatSpec with Matchers with SparkContexts {

  "A RegexConstraint" should "succeed if all values satisfy the regex" in {
    val column = "column"
    val regex = "^Hello"
    val check = Check(TestData.makeNullableStringDf(sql, List("Hello A", "Hello B", "Hello C"))).isMatchingRegex(column, regex)
    val constraint = check.constraints.head
    val result = RegexConstraintResult(
      constraint = RegexConstraint(column, regex),
      data = Some(RegexConstraintResultData(failedRows = 0L)),
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if all values satisfy the regex or are null" in {
    val column = "column"
    val regex = "^Hello"
    val check = Check(TestData.makeNullableStringDf(sql, List("Hello A", "Hello B", null))).isMatchingRegex(column, regex)
    val constraint = check.constraints.head
    val result = RegexConstraintResult(
      constraint = RegexConstraint(column, regex),
      data = Some(RegexConstraintResultData(failedRows = 0L)),
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if there is a row not satisfying the regex" in {
    val column = "column"
    val regex = "^Hello A$"
    val check = Check(TestData.makeNullableStringDf(sql, List("Hello A", "Hello A", "Hello B"))).isMatchingRegex(column, regex)
    val constraint = check.constraints.head
    val result = RegexConstraintResult(
      constraint = RegexConstraint(column, regex),
      data = Some(RegexConstraintResultData(failedRows = 1L)),
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "error if the column does not exist" in {
    val column = "notExisting"
    val regex = "^Hello"
    val check = Check(TestData.makeNullableStringDf(sql, List("Hello A", "Hello B", "Hello C"))).isMatchingRegex(column, regex)
    val constraint = check.constraints.head
    val result = check.run().constraintResults(constraint)
    result match {
      case RegexConstraintResult(
      RegexConstraint(_, _),
      None,
      constraintError: ConstraintError
      ) => {
        val analysisException = constraintError.throwable.asInstanceOf[AnalysisException]
        analysisException.message shouldBe "cannot resolve 'notExisting' given input columns column"
      }
    }
  }

  "A RegexConstraintResult" should "have the correct success message" in {
    val constraint = RegexConstraint("c", ".*")
    val result = RegexConstraintResult(
      constraint = constraint,
      data = Some(RegexConstraintResultData(failedRows = 0L)),
      status = ConstraintSuccess
    )
    result.message shouldBe "Column c matches .*"
  }

  it should "have the correct failure message (one row)" in {
    val constraint = RegexConstraint("c", ".*")
    val result = RegexConstraintResult(
      constraint = constraint,
      data = Some(RegexConstraintResultData(failedRows = 1L)),
      status = ConstraintFailure
    )
    result.message shouldBe "Column c contains 1 row that does not match .*"
  }

  it should "have the correct failure message (two rows)" in {
    val constraint = RegexConstraint("c", ".*")
    val result = RegexConstraintResult(
      constraint = constraint,
      data = Some(RegexConstraintResultData(failedRows = 2L)),
      status = ConstraintFailure
    )
    result.message shouldBe "Column c contains 2 rows that do not match .*"
  }

  it should "have the correct error message" in {
    val constraint = RegexConstraint("c", ".*")
    val result = RegexConstraintResult(
      constraint = constraint,
      data = None,
      status = ConstraintError(new IllegalArgumentException("error"))
    )
    result.message shouldBe "Checking whether column c matches .* failed: java.lang.IllegalArgumentException: error"
  }

  it should "throw an exception if it is created with an illegal combination of fields" in {
    intercept[IllegalConstraintResultException] {
      RegexConstraintResult(
        constraint = RegexConstraint("c", ".*"),
        status = ConstraintFailure,
        data = None
      )
    }
  }

}
