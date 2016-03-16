package de.frosner.ddq.constraints

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
    }
  }
}

case class AnyOfConstraintResultData(failedRows: Long)
