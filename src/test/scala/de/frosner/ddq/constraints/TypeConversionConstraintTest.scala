package de.frosner.ddq.constraints

import de.frosner.ddq.core.Check
import de.frosner.ddq.testutils.{SparkContexts, TestData}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType}
import org.scalatest.{FlatSpec, Matchers}

class TypeConversionConstraintTest extends FlatSpec with Matchers with SparkContexts {

  "A TypeConversionConstraint" should "succeed if all elements can be converted" in {
    val column = "column"
    val targetType = IntegerType
    val check = Check(TestData.makeNullableStringDf(sql, List("1", "2", "3"))).isConvertibleTo(column, targetType)
    val constraint = check.constraints.head
    val result = TypeConversionConstraintResult(
      constraint = TypeConversionConstraint(column, targetType),
      data = Some(TypeConversionConstraintResultData(
        originalType = StringType,
        failedRows = 0L
      )),
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if all elements can be converted or are null" in {
    val column = "column"
    val targetType = DoubleType
    val check = Check(TestData.makeNullableStringDf(sql, List("1.0", "2.0", null))).isConvertibleTo(column, targetType)
    val constraint = check.constraints.head
    val result = TypeConversionConstraintResult(
      constraint = TypeConversionConstraint(column, targetType),
      data = Some(TypeConversionConstraintResultData(
        originalType = StringType,
        failedRows = 0L
      )),
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if at least one element cannot be converted" in {
    val column = "column"
    val targetType = LongType
    val check = Check(TestData.makeNullableStringDf(sql, List("1", "2", "hallo", "test"))).isConvertibleTo(column, targetType)
    val constraint = check.constraints.head
    val result = TypeConversionConstraintResult(
      constraint = TypeConversionConstraint(column, targetType),
      data = Some(TypeConversionConstraintResultData(
        originalType = StringType,
        failedRows = 2L
      )),
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "error the column does not exist" in {
    val column = "notExisting"
    val targetType = IntegerType
    val check = Check(TestData.makeNullableStringDf(sql, List("1", "2", "3"))).isConvertibleTo(column, targetType)
    val constraint = check.constraints.head
    val result = check.run().constraintResults(constraint)
    result match {
      case TypeConversionConstraintResult(
        TypeConversionConstraint("notExisting", IntegerType),
        None,
        constraintError: ConstraintError
      ) => {
        val analysisException = constraintError.throwable.asInstanceOf[AnalysisException]
        analysisException.message shouldBe "cannot resolve 'notExisting' given input columns column"
      }
    }
  }

  "A TypeConversionConstraintResult" should "have the correct success message" in {
    val constraint = TypeConversionConstraint("c", IntegerType)
    val result = TypeConversionConstraintResult(
      constraint = constraint,
      data = Some(TypeConversionConstraintResultData(
        originalType = StringType,
        failedRows = 0L
      )),
      status = ConstraintSuccess
    )
    result.message shouldBe "Column c can be converted from StringType to IntegerType."
  }

  it should "have the correct failure message (one row)" in {
    val constraint = TypeConversionConstraint("c", IntegerType)
    val result = TypeConversionConstraintResult(
      constraint = constraint,
      data = Some(TypeConversionConstraintResultData(
        originalType = StringType,
        failedRows = 1L
      )),
      status = ConstraintFailure
    )
    result.message shouldBe "Column c cannot be converted from StringType to IntegerType. 1 row could not be converted."
  }

  it should "have the correct failure message (multiple rows)" in {
    val constraint = TypeConversionConstraint("c", IntegerType)
    val result = TypeConversionConstraintResult(
      constraint = constraint,
      data = Some(TypeConversionConstraintResultData(
        originalType = StringType,
        failedRows = 2L
      )),
      status = ConstraintFailure
    )
    result.message shouldBe "Column c cannot be converted from StringType to IntegerType. 2 rows could not be converted."
  }

  it should "have the correct error message" in {
    val constraint = TypeConversionConstraint("c", IntegerType)
    val result = TypeConversionConstraintResult(
      constraint = constraint,
      data = None,
      status = ConstraintError(new IllegalArgumentException("error"))
    )
    result.message shouldBe "Checking whether column c can be converted to IntegerType failed: " +
      "java.lang.IllegalArgumentException: error"
  }


}
