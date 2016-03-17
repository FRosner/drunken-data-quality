package de.frosner.ddq.constraints

import de.frosner.ddq.core.Check
import de.frosner.ddq.testutils.{TestData, SparkContexts}
import org.scalatest.{FlatSpec, Matchers}

class FunctionalDependencyConstraintTest extends FlatSpec with Matchers with SparkContexts {

  "A FunctionalDependencyConstraint" should "succeed if the column values in the determinant set always correspond to " +
    "the column values in the dependent set" in {
    val determinantSet = Seq("column1", "column2")
    val dependentSet = Seq("column3")
    val check = Check(TestData.makeIntegersDf(sql,
      List(1, 2, 1, 1),
      List(9, 9, 9, 2),
      List(9, 9, 9, 3),
      List(3, 4, 3, 4),
      List(7, 7, 7, 5)
    )).hasFunctionalDependency(determinantSet, dependentSet)
    val constraint = check.constraints.head
    val result = FunctionalDependencyConstraintResult(
      constraint = FunctionalDependencyConstraint(determinantSet, dependentSet),
      failedRows = 0L,
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed also for dependencies where the determinant and dependent sets are not distinct" in {
    val determinantSet = Seq("column1", "column2")
    val dependentSet = Seq("column2", "column3")
    val check = Check(TestData.makeIntegersDf(sql,
      List(1, 2, 3, 1),
      List(9, 9, 9, 2),
      List(9, 9, 9, 3),
      List(3, 4, 3, 4),
      List(7, 7, 7, 5)
    )).hasFunctionalDependency(determinantSet, dependentSet)
    val constraint = check.constraints.head
    val result = FunctionalDependencyConstraintResult(
      constraint = FunctionalDependencyConstraint(determinantSet, dependentSet),
      failedRows = 0L,
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "succeed if the determinant and dependent sets are equal" in {
    val determinantSet = Seq("column")
    val dependentSet = determinantSet
    val check = Check(TestData.makeIntegerDf(sql,
      List(1, 2, 3)
    )).hasFunctionalDependency(determinantSet, dependentSet)
    val constraint = check.constraints.head
    val result = FunctionalDependencyConstraintResult(
      constraint = FunctionalDependencyConstraint(determinantSet, dependentSet),
      failedRows = 0L,
      status = ConstraintSuccess
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail if the column values in the determinant set don't always correspond to the column values in the dependent set" in {
    val determinantSet = Seq("column1", "column2")
    val dependentSet = Seq("column3")
    val check = Check(TestData.makeIntegersDf(sql,
      List(1, 2, 1, 1),
      List(9, 9, 9, 1),
      List(9, 9, 8, 1),
      List(3, 4, 3, 1),
      List(7, 7, 7, 1),
      List(7, 7, 6, 1)
    )).hasFunctionalDependency(determinantSet, dependentSet)
    val constraint = check.constraints.head
    val result = FunctionalDependencyConstraintResult(
      constraint = FunctionalDependencyConstraint(determinantSet, dependentSet),
      failedRows = 2L,
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "fail also for dependencies where the determinant and dependent sets are not distinct" in {
    val determinantSet = Seq("column1", "column2")
    val dependentSet = Seq("column2", "column3")
    val check = Check(TestData.makeIntegersDf(sql,
      List(1, 2, 1, 1),
      List(9, 9, 9, 1),
      List(9, 9, 8, 1),
      List(3, 4, 3, 1),
      List(7, 7, 7, 1)
    )).hasFunctionalDependency(determinantSet, dependentSet)
    val constraint = check.constraints.head
    val result = FunctionalDependencyConstraintResult(
      constraint = FunctionalDependencyConstraint(determinantSet, dependentSet),
      failedRows = 1L,
      status = ConstraintFailure
    )
    check.run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "require the determinant set to be non-empty" in {
    intercept[IllegalArgumentException] {
      Check(TestData.makeIntegersDf(sql,
        List(1, 2, 3),
        List(4, 5, 6)
      )).hasFunctionalDependency(Seq.empty, Seq("column0"))
    }
  }

  it should "require the dependent set to be non-empty" in {
    intercept[IllegalArgumentException] {
      Check(TestData.makeIntegersDf(sql,
        List(1, 2, 3),
        List(4, 5, 6)
      )).hasFunctionalDependency(Seq("column0"), Seq.empty)
    }
  }

  "A FunctionalDependencyConstraintResult" should "have the correct success message (one/one column)" in {
    val constraint = FunctionalDependencyConstraint(Seq("a"), Seq("c"))
    val result = FunctionalDependencyConstraintResult(constraint, 0L, ConstraintSuccess)
    result.message shouldBe "Column c is functionally dependent on a."
  }

  it should "have the correct success message (one/multiple columns)" in {
    val constraint = FunctionalDependencyConstraint(Seq("a"), Seq("c", "d"))
    val result = FunctionalDependencyConstraintResult(constraint, 0L, ConstraintSuccess)
    result.message shouldBe "Columns c, d are functionally dependent on a."
  }

  it should "have the correct success message (multiple/one columns)" in {
    val constraint = FunctionalDependencyConstraint(Seq("a", "b"), Seq("c"))
    val result = FunctionalDependencyConstraintResult(constraint, 0L, ConstraintSuccess)
    result.message shouldBe "Column c is functionally dependent on a, b."
  }

  it should "have the correct success message (multiple/multiple columns)" in {
    val constraint = FunctionalDependencyConstraint(Seq("a", "b"), Seq("c", "d"))
    val result = FunctionalDependencyConstraintResult(constraint, 0L, ConstraintSuccess)
    result.message shouldBe "Columns c, d are functionally dependent on a, b."
  }

  it should "have the correct failure message (one/one column, one row)" in {
    val constraint = FunctionalDependencyConstraint(Seq("a"), Seq("c"))
    val result = FunctionalDependencyConstraintResult(constraint, 1L, ConstraintFailure)
    result.message shouldBe "Column c is not functionally dependent on a (1 violating determinant value)."
  }

  it should "have the correct failure message (one/multiple columns, multiple rows)" in {
    val constraint = FunctionalDependencyConstraint(Seq("a"), Seq("c", "d"))
    val result = FunctionalDependencyConstraintResult(constraint, 2L, ConstraintFailure)
    result.message shouldBe "Columns c, d are not functionally dependent on a (2 violating determinant values)."
  }

  it should "have the correct failure message (multiple/one columns, one row)" in {
    val constraint = FunctionalDependencyConstraint(Seq("a", "b"), Seq("c"))
    val result = FunctionalDependencyConstraintResult(constraint, 1L, ConstraintFailure)
    result.message shouldBe "Column c is not functionally dependent on a, b (1 violating determinant value)."
  }

  it should "have the correct failure message (multiple/multiple columns, multiple rows)" in {
    val constraint = FunctionalDependencyConstraint(Seq("a", "b"), Seq("c", "d"))
    val result = FunctionalDependencyConstraintResult(constraint, 5L, ConstraintFailure)
    result.message shouldBe "Columns c, d are not functionally dependent on a, b (5 violating determinant values)."
  }

}
