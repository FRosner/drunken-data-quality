package de.frosner.ddq.constraints

case class DateFormatConstraintResult(constraint: DateFormatConstraint,
                                      failedRows: Long,
                                      status: ConstraintStatus) extends ConstraintResult[DateFormatConstraint] {
  val message: String = {
    val format = constraint.format.toPattern
    val columnName = constraint.columnName
    val pluralS = if (failedRows == 1) "" else "s"
    val verb = if (failedRows == 1) "is" else "are"
    status match {
      case ConstraintSuccess => s"Column $columnName is formatted by $format."
      case ConstraintFailure => s"Column $columnName contains $failedRows row$pluralS that $verb not formatted by $format."
    }
  }
}
