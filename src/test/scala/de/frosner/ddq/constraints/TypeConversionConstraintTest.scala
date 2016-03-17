package de.frosner.ddq.constraints

import de.frosner.ddq.core.Check
import de.frosner.ddq.testutils.{TestData, SparkContexts}
import org.apache.spark.sql.types.{LongType, DoubleType, StringType, IntegerType}
import org.scalatest.{FlatSpec, Matchers}

class TypeConversionConstraintTest extends FlatSpec with Matchers with SparkContexts {

  "A TypeConversionConstraint" should "succeed if all elements can be converted" in {
    val column = "column"
    val targetType = IntegerType
    val check = Check(TestData.makeNullableStringDf(sql, List("1", "2", "3"))).isConvertibleTo(column, targetType)
    val constraint = check.constraints.head
    val result = TypeConversionConstraintResult(
      constraint = TypeConversionConstraint(column, targetType),
      originalType = StringType,
      failedRows = 0L,
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
      originalType = StringType,
      failedRows = 0L,
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
      originalType = StringType,
      failedRows = 2L,
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  "A TypeConversionConstraintResult" should "have the correct success message" in {
    val constraint = TypeConversionConstraint("c", IntegerType)
    val result = TypeConversionConstraintResult(constraint, StringType, 0L, ConstraintSuccess)
    result.message shouldBe "Column c can be converted from StringType to IntegerType."
  }

  it should "have the correct failure message (one row)" in {
    val constraint = TypeConversionConstraint("c", IntegerType)
    val result = TypeConversionConstraintResult(constraint, StringType, 1L, ConstraintFailure)
    result.message shouldBe "Column c cannot be converted from StringType to IntegerType. 1 row could not be converted."
  }

  it should "have the correct failure message (multiple rows)" in {
    val constraint = TypeConversionConstraint("c", IntegerType)
    val result = TypeConversionConstraintResult(constraint, StringType, 2L, ConstraintFailure)
    result.message shouldBe "Column c cannot be converted from StringType to IntegerType. 2 rows could not be converted."
  }

}
