package de.frosner.ddq.constraints

case class ColumnColumnConstraintResult(constraint: ColumnColumnConstraint,
                                        data: Option[ColumnColumnConstraintResultData],
                                        status: ConstraintStatus) extends ConstraintResult[ColumnColumnConstraint] {

  val message: String = {
    val constraintString = constraint.constraintColumn.toString
    val maybeViolatingRows = data.map(_.failedRows)
    val maybePluralS = maybeViolatingRows.map(violatingRows => if (violatingRows == 1) "" else "s")
    (status, maybeViolatingRows, maybePluralS) match {
      case (ConstraintSuccess, Some(0), _) =>
        s"Constraint $constraintString is satisfied."
      case (ConstraintFailure, Some(violatingRows), Some(pluralS)) =>
        s"$violatingRows row$pluralS did not satisfy constraint $constraintString."
      case (ConstraintError(throwable), None, None) =>
        s"Checking constraint $constraintString failed: $throwable"
    }
  }

}

case class ColumnColumnConstraintResultData(failedRows: Long)
