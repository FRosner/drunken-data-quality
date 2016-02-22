package de.frosner.ddq.constraints

case class ForeignKeyConstraintResult(constraint: ForeignKeyConstraint,
                                      numNonMatchingRefs: Option[Long],
                                      status: ConstraintStatus) extends ConstraintResult[ForeignKeyConstraint] {
  val message: String = {
    val referenceTable = constraint.referenceTable
    val columnNames = constraint.columnNames
    val columnsString = columnNames.map { case (baseCol, refCol) => baseCol + "->" + refCol }.mkString(", ")
    val isPlural = columnNames.length > 1
    val (columnDo, columnDefine, columnIs, columnPluralS) =
      if (isPlural) ("do", "define", "are", "s") else ("does", "defines", "is", "")
    val columnNoun = "Column" + columnPluralS
    (status, numNonMatchingRefs) match {
      case (ConstraintSuccess, _) => s"$columnNoun $columnsString $columnDefine a foreign key " +
        s"pointing to the reference table $referenceTable."
      case (ConstraintFailure, None) => s"$columnNoun $columnsString $columnIs not a key in the reference table."
      case (ConstraintFailure, Some(nonMatching)) => {
        val (rowsNoun, rowsDo) = if (nonMatching != 1) ("rows", "do") else ("row", "does")
        s"$columnNoun $columnsString $columnDo not define a foreign key " +
          s"pointing to $referenceTable. $nonMatching $rowsNoun $rowsDo not match."
      }
    }
  }
}
