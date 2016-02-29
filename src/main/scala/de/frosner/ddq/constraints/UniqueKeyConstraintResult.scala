package de.frosner.ddq.constraints

case class UniqueKeyConstraintResult(constraint: UniqueKeyConstraint,
                                     numNonUniqueTuples: Long,
                                     status: ConstraintStatus) extends ConstraintResult[UniqueKeyConstraint] {
  val message: String = {
    val columnNames = constraint.columnNames
    val columnsString = columnNames.mkString(", ")
    val isPlural = columnNames.length > 1
    val columnNoun = "Column" + (if (isPlural) "s" else "")
    val columnVerb = if (isPlural) "are" else "is"
    val pluralS = if (numNonUniqueTuples != 1) "s" else ""
    status match {
      case ConstraintSuccess => s"$columnNoun $columnsString $columnVerb a key."
      case ConstraintFailure => s"$columnNoun $columnsString $columnVerb not a key ($numNonUniqueTuples non-unique tuple$pluralS)."
    }
  }
}
