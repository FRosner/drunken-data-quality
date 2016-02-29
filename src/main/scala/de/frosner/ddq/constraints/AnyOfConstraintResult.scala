package de.frosner.ddq.constraints

case class AnyOfConstraintResult(constraint: AnyOfConstraint,
                                 failedRows: Long,
                                 status: ConstraintStatus) extends ConstraintResult[AnyOfConstraint] {
  val message: String = {
    val allowed = constraint.allowedValues
    val columnName = constraint.columnName
    val (pluralS, verb) = if (failedRows == 1) ("", "is") else ("s", "are")
    status match {
      case ConstraintSuccess => s"Column $columnName contains only values in $allowed."
      case ConstraintFailure => s"Column $columnName contains $failedRows row$pluralS that $verb not in $allowed."
    }
  }
}
