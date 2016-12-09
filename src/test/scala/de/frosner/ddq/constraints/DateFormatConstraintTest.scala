package de.frosner.ddq.constraints

import java.text.SimpleDateFormat

import de.frosner.ddq.core.Check
import de.frosner.ddq.testutils.{SparkContexts, TestData}
import org.apache.spark.sql.AnalysisException
import org.scalatest.{FlatSpec, Matchers}

class DateFormatConstraintTest extends FlatSpec with Matchers with SparkContexts {

  "A DateFormatConstraint" should "succeed if all elements can be converted to Date" in {
    val column = "column"
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val check = Check(TestData.makeNullableStringDf(spark, List("2000-11-23 11:50:10", "2000-5-23 11:50:10", "2000-02-23 11:11:11"))).
      isFormattedAsDate(column, format)
    val constraint = check.constraints.head
    val result = DateFormatConstraintResult(
      constraint = DateFormatConstraint(column, format),
      data = Some(DateFormatConstraintResultData(failedRows = 0)),
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if all elements can be converted to Date or are null" in {
    val column = "column"
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val check = Check(TestData.makeNullableStringDf(spark, List("2000-11-23 11:50:10", null, "2000-02-23 11:11:11"))).
      isFormattedAsDate(column, format)
    val constraint = check.constraints.head
    val result = DateFormatConstraintResult(
      constraint = DateFormatConstraint(column, format),
      data = Some(DateFormatConstraintResultData(failedRows = 0)),
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if at least one element cannot be converted to Date" in {
    val column = "column"
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val check =  Check(TestData.makeNullableStringDf(spark, List("2000-11-23 11:50:10", "abc", "2000-15-23 11:11:11"))).
      isFormattedAsDate(column, format)
    val constraint = check.constraints.head
    val result = DateFormatConstraintResult(
      constraint = DateFormatConstraint(column, format),
      data = Some(DateFormatConstraintResultData(failedRows = 1L)),
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "error if the condition references a non-existing column" in {
    val column = "notExisting"
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val check =  Check(TestData.makeNullableStringDf(spark, List("2000-11-23 11:50:10", "abc", "2000-15-23 11:11:11"))).
      isFormattedAsDate(column, format)
    val constraint = check.constraints.head
    val result = check.run().constraintResults(constraint)
    result match {
      case DateFormatConstraintResult(
      DateFormatConstraint("notExisting", _),
      None,
      constraintError: ConstraintError
      ) => {
        val analysisException = constraintError.throwable.asInstanceOf[AnalysisException]
        analysisException.message shouldBe "cannot resolve '`notExisting`' given input columns: [column]"
      }
    }
  }

  "A DateFormatConstraintResult" should "have the correct success message" in {
    val constraint = DateFormatConstraint("c", new SimpleDateFormat("yyyy"))
    val result = DateFormatConstraintResult(
      constraint = constraint,
      data = Some(DateFormatConstraintResultData(failedRows = 0L)),
      status = ConstraintSuccess
    )
    result.message shouldBe "Column c is formatted by yyyy."
  }

  it should "have the correct failure message (one row)" in {
    val constraint = DateFormatConstraint("c", new SimpleDateFormat("yyyy"))
    val result = DateFormatConstraintResult(
      constraint = constraint,
      data = Some(DateFormatConstraintResultData(failedRows = 1L)),
      status = ConstraintFailure
    )
    result.message shouldBe "Column c contains 1 row that is not formatted by yyyy."
  }

  it should "have the correct failure message (multiple rows)" in {
    val constraint = DateFormatConstraint("c", new SimpleDateFormat("yyyy"))
    val result = DateFormatConstraintResult(
      constraint = constraint,
      data = Some(DateFormatConstraintResultData(failedRows = 2L)),
      status = ConstraintFailure
    )
    result.message shouldBe "Column c contains 2 rows that are not formatted by yyyy."
  }

  it should "have the correct error message" in {
    val constraint = DateFormatConstraint("c", new SimpleDateFormat("yyyy"))
    val result = DateFormatConstraintResult(
      constraint = constraint,
      data = None,
      status = ConstraintError(new IllegalArgumentException("column c not found"))
    )
    result.message shouldBe "Checking whether column c is formatted by yyyy failed: " +
      "java.lang.IllegalArgumentException: column c not found"
  }

  it should "throw an exception if it is created with an illegal combination of fields" in {
    intercept[IllegalConstraintResultException] {
      DateFormatConstraintResult(
        constraint = DateFormatConstraint("c", new SimpleDateFormat("yyyy")),
        status = ConstraintFailure,
        data = None
      )
    }
  }

}
