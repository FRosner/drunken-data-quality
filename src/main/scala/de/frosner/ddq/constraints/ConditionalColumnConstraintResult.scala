package de.frosner.ddq.constraints

case class ConditionalColumnConstraintResult(constraint: ConditionalColumnConstraint,
                                             violatingRows: Long,
                                             status: ConstraintStatus) extends ConstraintResult[ConditionalColumnConstraint] {

  val message: String = {
    val constraintString = s"${constraint.statement} -> ${constraint.implication}"
    val pluralS = if (violatingRows == 1) "" else "s"
    status match {
      case ConstraintSuccess => s"Constraint $constraintString is satisfied."
      case ConstraintFailure => s"$violatingRows row$pluralS did not satisfy constraint $constraintString."
    }
  }

}
