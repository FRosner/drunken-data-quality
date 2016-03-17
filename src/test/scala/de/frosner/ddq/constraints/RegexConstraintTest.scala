package de.frosner.ddq.constraints

import de.frosner.ddq.core.Check
import de.frosner.ddq.testutils.{TestData, SparkContexts}
import org.scalatest.{FlatSpec, Matchers}

class RegexConstraintTest extends FlatSpec with Matchers with SparkContexts {

  "A RegexConstraint" should "succeed if all values satisfy the regex" in {
    val column = "column"
    val regex = "^Hello"
    val check = Check(TestData.makeNullableStringDf(sql, List("Hello A", "Hello B", "Hello C"))).isMatchingRegex(column, regex)
    val constraint = check.constraints.head
    val result = RegexConstraintResult(
      constraint = RegexConstraint(column, regex),
      failedRows = 0L,
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
      failedRows = 0L,
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
      failedRows = 1L,
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "A RegexConstraintResult" should "have the correct success message" in {
    val constraint = RegexConstraint("c", ".*")
    val result = RegexConstraintResult(constraint, 0L, ConstraintSuccess)
    result.message shouldBe "Column c matches .*"
  }

  it should "have the correct failure message (one row)" in {
    val constraint = RegexConstraint("c", ".*")
    val result = RegexConstraintResult(constraint, 1L, ConstraintFailure)
    result.message shouldBe "Column c contains 1 row that does not match .*"
  }

  it should "have the correct failure message (two rows)" in {
    val constraint = RegexConstraint("c", ".*")
    val result = RegexConstraintResult(constraint, 2L, ConstraintFailure)
    result.message shouldBe "Column c contains 2 rows that do not match .*"
  }

}
