package de.frosner.ddq.constraints

case class ColumnColumnConstraintResult(constraint: ColumnColumnConstraint,
                                        violatingRows: Long,
                                        status: ConstraintStatus) extends ConstraintResult[ColumnColumnConstraint] {

  val message: String = {
    val constraintString = constraint.constraintColumn.toString
    val pluralS = if (violatingRows == 1) "" else "s"
    status match {
      case ConstraintSuccess => s"Constraint $constraintString is satisfied."
      case ConstraintFailure => s"$violatingRows row$pluralS did not satisfy constraint $constraintString."
    }
  }

}
