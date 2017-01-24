package de.frosner.ddq.constraints

import de.frosner.ddq.core.Check
import de.frosner.ddq.testutils.{SparkContexts, TestData}
import org.apache.spark.sql.AnalysisException
import org.scalatest.{FlatSpec, Matchers}

class ExactEqualityConstraintTest extends FlatSpec with Matchers with SparkContexts {

  "An ExactEqualityConstraint" should "succeed if the two dataframes are equal" in {
    val df = TestData.makeNullableStringDf(spark, List("a", "b", "c"))
    val other = TestData.makeNullableStringDf(spark, List("a", "b", "c"))
    val check = Check(df).isEqualTo(other)
    val constraint = check.constraints.head
    val result = ExactEqualityConstraintResult(
      constraint = ExactEqualityConstraint(other),
      data = Some(ExactEqualityConstraintData(0L, 0L)),
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if the two dataframes are equal and contain null values" in {
    val df = TestData.makeNullableStringDf(spark, List("a", "b", null))
    val other = TestData.makeNullableStringDf(spark, List("a", "b", null))
    val check = Check(df).isEqualTo(other)
    val constraint = check.constraints.head
    val result = ExactEqualityConstraintResult(
      constraint = ExactEqualityConstraint(other),
      data = Some(ExactEqualityConstraintData(0L, 0L)),
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if the two dataframes have the same distinct content but different number of rows" in {
    val df = TestData.makeNullableStringDf(spark, List("a", "b", null))
    val other = TestData.makeNullableStringDf(spark, List("a", "b", null, null))
    val check = Check(df).isEqualTo(other)
    val constraint = check.constraints.head
    val result = ExactEqualityConstraintResult(
      constraint = ExactEqualityConstraint(other),
      data = Some(ExactEqualityConstraintData(1L, 1L)),
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if the two dataframes have different content but the same number of rows" in {
    val df = TestData.makeNullableStringDf(spark, List("a", "b", null))
    val other = TestData.makeNullableStringDf(spark, List("a", "c", null))
    val check = Check(df).isEqualTo(other)
    val constraint = check.constraints.head
    val result = ExactEqualityConstraintResult(
      constraint = ExactEqualityConstraint(other),
      data = Some(ExactEqualityConstraintData(1L, 1L)),
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "error if the two dataframes have equal content but different column names" in {
    val df = spark.createDataFrame(List(Customer1(5, "Frank")))
    val other = spark.createDataFrame(List(Customer3(5, "Frank")))
    val check = Check(df).isEqualTo(other)
    val constraint = check.constraints.head
    val result = check.run().constraintResults(constraint)
    result match {
      case ExactEqualityConstraintResult(
        ExactEqualityConstraint(_),
        None,
        constraintError: ConstraintError
      ) => {
        val exception = constraintError.throwable.asInstanceOf[IllegalArgumentException]
        exception.getMessage shouldBe "Schemas do not match"
      }
    }
  }

  it should "error if the two dataframes have equal content but different column types" in {
    val df = spark.createDataFrame(List(Customer4(5L, "Frank")))
    val other = spark.createDataFrame(List(Customer3(5, "Frank")))
    val check = Check(df).isEqualTo(other)
    val constraint = check.constraints.head
    val result = check.run().constraintResults(constraint)
    result match {
      case ExactEqualityConstraintResult(
      ExactEqualityConstraint(_),
      None,
      constraintError: ConstraintError
      ) => {
        val exception = constraintError.throwable.asInstanceOf[IllegalArgumentException]
        exception.getMessage shouldBe "Schemas do not match"
      }
    }
  }

  it should "error if the two dataframes are equal but have different ordering in columns" in {
    val df = spark.createDataFrame(List(Customer1(5, "Frank")))
    val other = spark.createDataFrame(List(Customer2("Frank", 5)))
    val check = Check(df).isEqualTo(other)
    val constraint = check.constraints.head
    val result = check.run().constraintResults(constraint)
    result match {
      case ExactEqualityConstraintResult(
      ExactEqualityConstraint(_),
      None,
      constraintError: ConstraintError
      ) => {
        val exception = constraintError.throwable.asInstanceOf[IllegalArgumentException]
        exception.getMessage shouldBe "Schemas do not match"
      }
    }
  }

  "A ExactEqualityConstraintResult" should "have the correct success message" in {
    val df = TestData.makeNullableStringDf(spark, List("a", "b", "c"))
    val result = ExactEqualityConstraintResult(
      constraint = ExactEqualityConstraint(df),
      data = Some(ExactEqualityConstraintData(0L, 0L)),
      status = ConstraintSuccess
    )
    result.message shouldBe s"It is equal to $df."
  }

  it should "have the correct failure message" in {
    val df = TestData.makeNullableStringDf(spark, List("a", "b", "c"))
    val result = ExactEqualityConstraintResult(
      constraint = ExactEqualityConstraint(df),
      data = Some(ExactEqualityConstraintData(1L, 2L)),
      status = ConstraintFailure
    )
    result.message shouldBe s"It is not equal (1 distinct count row is present in the checked dataframe but not in the other and 2 distinct count rows are present in the other dataframe but not in the checked one) to $df."
  }

  it should "have the correct error message" in {
    val df = TestData.makeNullableStringDf(spark, List("a", "b", "c"))
    val result = ExactEqualityConstraintResult(
      constraint = ExactEqualityConstraint(df),
      data = None,
      status = ConstraintError(new IllegalArgumentException("exception"))
    )
    result.message shouldBe s"Checking equality with $df failed: " +
      "java.lang.IllegalArgumentException: exception"
  }

  it should "throw an exception if it is created with an illegal combination of fields" in {
    intercept[IllegalConstraintResultException] {
      ExactEqualityConstraintResult(
        constraint = ExactEqualityConstraint(TestData.makeNullableStringDf(spark, List("a", "b", "c"))),
        data = None,
        status = ConstraintFailure
      )
    }
  }
  
}

case class Customer1(id: Int, name: String)
case class Customer2(name: String, id: Int)
case class Customer3(idd: Int, name: String)
case class Customer4(idd: Long, name: String)