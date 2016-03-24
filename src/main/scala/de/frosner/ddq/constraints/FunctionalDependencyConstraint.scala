package de.frosner.ddq.constraints

import org.apache.spark.sql.{Column, DataFrame}

import scala.util.Try

case class FunctionalDependencyConstraint(determinantSet: Seq[String],
                                          dependentSet: Seq[String]) extends Constraint {

  require(determinantSet.nonEmpty, "determinantSet must not be empty")
  require(dependentSet.nonEmpty, "dependentSet must not be empty")

  val fun = (df: DataFrame) => {
    val determinantColumns = determinantSet.map(columnName => new Column(columnName))
    val dependentColumns = dependentSet.map(columnName => new Column(columnName))
    val maybeRelevantSelection = Try(df.select(determinantColumns ++ dependentColumns: _*))

    val maybeDeterminantValueCounts = maybeRelevantSelection.map(_.distinct.groupBy(determinantColumns: _*).count)
    val maybeViolatingDeterminantValuesCount = maybeDeterminantValueCounts.map(_.filter(new Column("count") !== 1).count)
    FunctionalDependencyConstraintResult(
      constraint = this,
      data = maybeViolatingDeterminantValuesCount.toOption.map(FunctionalDependencyConstraintResultData),
      status = ConstraintUtil.tryToStatus[Long](maybeViolatingDeterminantValuesCount, _ == 0)
    )
  }

}

case class FunctionalDependencyConstraintResult(constraint: FunctionalDependencyConstraint,
                                                data: Option[FunctionalDependencyConstraintResultData],
                                                status: ConstraintStatus) extends ConstraintResult[FunctionalDependencyConstraint] {

  val message: String = {
    val maybeFailedRows = data.map(_.failedRows)
    val maybeRowPluralS = maybeFailedRows.map(failedRows => if (failedRows == 1) "" else "s")
    val dependentSet = constraint.dependentSet
    val determinantString = s"${constraint.determinantSet.mkString(", ")}"
    val dependentString = s"${dependentSet.mkString(", ")}"
    val (columnPluralS, columnVerb) = if (dependentSet.size == 1) ("", "is") else ("s", "are")
    (status, maybeFailedRows, maybeRowPluralS) match {
      case (ConstraintSuccess, Some(0), _) =>
        s"Column$columnPluralS $dependentString $columnVerb functionally dependent on $determinantString."
      case (ConstraintFailure, Some(failedRows), Some(rowPluralS)) =>
        s"Column$columnPluralS $dependentString $columnVerb not functionally dependent on " +
        s"$determinantString ($failedRows violating determinant value$rowPluralS)."
      case (ConstraintError(throwable), None, None) =>
        s"Checking whether column$columnPluralS $dependentString $columnVerb functionally " +
          s"dependent on $determinantString failed: $throwable"
    }
  }

}

case class FunctionalDependencyConstraintResultData(failedRows: Long)
