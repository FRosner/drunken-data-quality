package de.frosner.ddq.constraints

case class AlwaysNullConstraintResult(constraint: AlwaysNullConstraint,
                                      nonNullRows: Long,
                                      status: ConstraintStatus) extends ConstraintResult[AlwaysNullConstraint] {
  val message: String = {
    val columnName = constraint.columnName
    val pluralS = if (nonNullRows == 1) "" else "s"
    status match {
      case ConstraintSuccess => s"Column $columnName is always null."
      case ConstraintFailure => s"Column $columnName contains $nonNullRows non-null row$pluralS (should always be null)."
    }
  }
}
