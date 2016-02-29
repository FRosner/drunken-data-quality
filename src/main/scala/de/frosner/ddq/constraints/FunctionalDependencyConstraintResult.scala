package de.frosner.ddq.constraints

case class FunctionalDependencyConstraintResult(constraint: FunctionalDependencyConstraint,
                                                failedRows: Long,
                                                status: ConstraintStatus) extends ConstraintResult[FunctionalDependencyConstraint] {
  val message: String = {
    val rowPluralS = if (failedRows == 1) "" else "s"
    val dependentSet = constraint.dependentSet
    val determinantString = s"${constraint.determinantSet.mkString(", ")}"
    val dependentString = s"${dependentSet.mkString(", ")}"
    val (columnPluralS, columnVerb) = if (dependentSet.size == 1) ("", "is") else ("s", "are")
    status match {
      case ConstraintSuccess => s"Column$columnPluralS $dependentString $columnVerb functionally dependent on $determinantString."
      case ConstraintFailure => s"Column$columnPluralS $dependentString $columnVerb not functionally dependent on " +
        s"$determinantString ($failedRows violating determinant value$rowPluralS)."
    }
  }
}
