package de.frosner.ddq.constraints

case class NeverNullConstraintResult(constraint: NeverNullConstraint,
                                     nullRows: Long,
                                     status: ConstraintStatus) extends ConstraintResult[NeverNullConstraint] {
  val message: String = {
    val columnName = constraint.columnName
    val pluralS = if (nullRows == 1) "" else "s"
    val verb = if (nullRows == 1) "is" else "are"
    status match {
      case ConstraintSuccess => s"Column $columnName is never null."
      case ConstraintFailure => s"Column $columnName contains $nullRows row$pluralS that $verb null (should never be null)."
    }
  }
}
