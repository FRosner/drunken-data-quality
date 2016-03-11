package de.frosner.ddq.constraints

case class AlwaysNullConstraintResult(constraint: AlwaysNullConstraint,
                                      status: ConstraintStatus,
                                      data: Option[AlwaysNullConstraintResultData]
                                     ) extends ConstraintResult[AlwaysNullConstraint] {

  val message: String = {
    val columnName = constraint.columnName
    val maybeNonNullRows = data.map(_.nonNullRows)
    val maybePluralS = maybeNonNullRows.map(n => if (n == 1) "" else "s")
    (status, maybeNonNullRows, maybePluralS) match {
      case (ConstraintError(throwable), None, None) =>
        s"Checking $columnName for being always null failed: $throwable"
      case (ConstraintSuccess, _, Some(pluralS)) =>
        s"Column $pluralS is always null."
      case (ConstraintFailure, Some(nonNullRows), Some(pluralS)) =>
        s"Column $columnName contains $nonNullRows non-null row$pluralS (should always be null)."
    }
  }

}

case class AlwaysNullConstraintResultData(nonNullRows: Long)
