package de.frosner.ddq.constraints

case class RegexConstraintResult(constraint: RegexConstraint,
                                 failedRows: Long,
                                 status: ConstraintStatus) extends ConstraintResult[RegexConstraint] {
  val message: String = {
    val columnName = constraint.columnName
    val regex = constraint.regex
    val (pluralS, verb) = if (failedRows == 1) ("", "does") else ("s", "do")
    status match {
      case ConstraintSuccess => s"Column $columnName matches $regex"
      case ConstraintFailure => s"Column $columnName contains $failedRows row$pluralS that $verb not match $regex"
    }
  }
}
