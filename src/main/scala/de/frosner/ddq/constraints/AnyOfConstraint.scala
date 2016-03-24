package de.frosner.ddq.constraints

import org.apache.spark.sql.{Column, DataFrame}

import scala.util.Try

case class AnyOfConstraint(columnName: String, allowedValues: Set[Any]) extends Constraint {

  val fun = (df: DataFrame) => {
    val maybeError = Try(df.select(new Column(columnName))) // check if column is not ambiguous
    val maybeColumnIndex = maybeError.map(_ => df.columns.indexOf(columnName))
    val maybeNotAllowedCount = maybeColumnIndex.map(columnIndex => df.rdd.filter(row => !row.isNullAt(columnIndex) &&
      !allowedValues.contains(row.get(columnIndex))).count)
    AnyOfConstraintResult(
      constraint = this,
      data = maybeNotAllowedCount.toOption.map(AnyOfConstraintResultData),
      status = ConstraintUtil.tryToStatus[Long](maybeNotAllowedCount, _ == 0)
    )
  }

}

case class AnyOfConstraintResult(constraint: AnyOfConstraint,
                                 data: Option[AnyOfConstraintResultData],
                                 status: ConstraintStatus) extends ConstraintResult[AnyOfConstraint] {
  val message: String = {
    val allowed = constraint.allowedValues
    val columnName = constraint.columnName
    val maybeFailedRows = data.map(_.failedRows)
    val maybePluralSAndVerb = maybeFailedRows.map(failedRows => if (failedRows == 1) ("", "is") else ("s", "are"))
    (status, maybeFailedRows, maybePluralSAndVerb) match {
      case (ConstraintSuccess, Some(0), Some((pluralS, verb))) =>
        s"Column $columnName contains only values in $allowed."
      case (ConstraintFailure, Some(failedRows), Some((pluralS, verb))) =>
        s"Column $columnName contains $failedRows row$pluralS that $verb not in $allowed."
      case (ConstraintError(throwable), None, None) =>
        s"Checking whether column $columnName contains only values in $allowed failed: $throwable"
      case default => throw IllegalConstraintResultException(this)
    }
  }
}

case class AnyOfConstraintResultData(failedRows: Long)
