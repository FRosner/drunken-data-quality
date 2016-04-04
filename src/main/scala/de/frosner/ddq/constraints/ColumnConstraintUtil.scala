package de.frosner.ddq.constraints

object ColumnConstraintUtil {

  private[constraints] def createColumnConstraintMessage[C <: Constraint](constraintResult: ConstraintResult[C],
                                                     status: ConstraintStatus,
                                                     constraintString: String,
                                                     maybeViolatingRows: Option[Long]): String = {
    val maybePluralS = maybeViolatingRows.map(violatingRows => if (violatingRows == 1) "" else "s")
    (status, maybeViolatingRows, maybePluralS) match {
      case (ConstraintSuccess, Some(0), _) =>
        s"Constraint $constraintString is satisfied."
      case (ConstraintFailure, Some(violatingRows), Some(pluralS)) =>
        s"$violatingRows row$pluralS did not satisfy constraint $constraintString."
      case (ConstraintError(throwable), None, None) =>
        s"Checking constraint $constraintString failed: $throwable"
      case default => throw IllegalConstraintResultException(constraintResult)
    }
  }

}
