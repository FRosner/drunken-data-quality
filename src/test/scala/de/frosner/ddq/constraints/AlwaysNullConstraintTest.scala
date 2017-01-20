package de.frosner.ddq.constraints

import com.holdenkarau.spark.testing.{SQLContextProvider, DataFrameSuiteBase, DatasetSuiteBase}
import de.frosner.ddq.core.Check
import de.frosner.ddq.testutils.{SparkContexts, TestData}
import org.apache.spark.sql.{DataFrame, AnalysisException}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class AlwaysNullConstraintTest extends DataFrameSuiteBase with Matchers with MockitoSugar {

  test("An AlwaysNullConstraint should succeed if the column is always null") {
    val column = "column"
    val check = Check(TestData.makeNullableStringDf(sqlContext, List(null, null, null))).isAlwaysNull(column)
    val constraint = check.constraints.head

    val constraintResults = check.run().constraintResults
    constraintResults should have size 1

    val constraintResult = constraintResults(constraint).asInstanceOf[AlwaysNullConstraintResult]
    constraintResult.constraint shouldBe AlwaysNullConstraint(column)
    constraintResult.status shouldBe ConstraintSuccess
    val Some(AlwaysNullConstraintResultData(nonNullRows, notNulls)) = constraintResult.data
    nonNullRows shouldBe 0
    notNulls.count shouldBe 0
  }

  test("An AlwaysNullConstraint should fail if the column is not always null") {
    val column = "column"
    val check = Check(TestData.makeNullableStringDf(sqlContext, List("a", null, null))).isAlwaysNull(column)
    val constraint = check.constraints.head

    val constraintResults = check.run().constraintResults
    constraintResults should have size 1

    val constraintResult = constraintResults(constraint).asInstanceOf[AlwaysNullConstraintResult]
    constraintResult.constraint shouldBe AlwaysNullConstraint(column)
    constraintResult.status shouldBe ConstraintFailure
    val Some(AlwaysNullConstraintResultData(nonNullRows, notNulls)) = constraintResult.data
    nonNullRows shouldBe 1
    assertDataFrameEquals(TestData.makeNullableStringDf(sqlContext, List("a")), notNulls)
  }

  test("An AlwaysNullConstraint should error if the column is not existing") {
    val column = "notExisting"
    val check = Check(TestData.makeNullableStringDf(sqlContext, List("a", null, null))).isAlwaysNull(column)
    val constraint = check.constraints.head
    val result = check.run().constraintResults(constraint)
    result match {
      case AlwaysNullConstraintResult(
      AlwaysNullConstraint("notExisting"),
      constraintError: ConstraintError,
      None
      ) => {
        val analysisException = constraintError.throwable.asInstanceOf[AnalysisException]
        analysisException.message shouldBe "cannot resolve 'notExisting' given input columns column"
      }
    }
  }

  test("An AlwaysNullConstraintResult should have the correct success message") {
    val constraint = AlwaysNullConstraint("c")
    val result = AlwaysNullConstraintResult(
      constraint = constraint,
      status = ConstraintSuccess,
      data = Some(AlwaysNullConstraintResultData(0L, mock[DataFrame]))
    )
    result.message shouldBe "Column c is always null."
  }

  test("An AlwaysNullConstraintResult should have the correct failure message (one row)") {
    val constraint = AlwaysNullConstraint("c")
    val result = AlwaysNullConstraintResult(
      constraint = constraint,
      status = ConstraintFailure,
      data = Some(AlwaysNullConstraintResultData(1L, mock[DataFrame]))
    )
    result.message shouldBe "Column c contains 1 non-null row (should always be null)."
  }

  test("An AlwaysNullConstraintResult should have the correct failure message (multiple rows)") {
    val constraint = AlwaysNullConstraint("c")
    val result = AlwaysNullConstraintResult(
      constraint = constraint,
      status = ConstraintFailure,
      data = Some(AlwaysNullConstraintResultData(2L, mock[DataFrame]))
    )
    result.message shouldBe "Column c contains 2 non-null rows (should always be null)."
  }

  test("An AlwaysNullConstraintResult should have the correct error message") {
    val constraint = AlwaysNullConstraint("c")
    val result = AlwaysNullConstraintResult(
      constraint = constraint,
      status = ConstraintError(new IllegalArgumentException("column c not found")),
      data = None
    )
    result.message shouldBe "Checking column c for being always null failed: " +
      "java.lang.IllegalArgumentException: column c not found"
  }

  test("An AlwaysNullConstraintResult should throw an exception if it is created with an illegal combination of fields") {
    intercept[IllegalConstraintResultException] {
      AlwaysNullConstraintResult(
        constraint = AlwaysNullConstraint("c"),
        status = ConstraintFailure,
        data = None
      )
    }
  }

}
