package de.frosner.ddq.constraints

case class JoinableConstraintResult(constraint: JoinableConstraint,
                                    distinctBefore: Long,
                                    matchingKeys: Long,
                                    status: ConstraintStatus) extends ConstraintResult[JoinableConstraint] {

  val matchRatio: Double = matchingKeys.toDouble / distinctBefore

  val message: String = {
    val columnNames = constraint.columnNames
    val columnsString = columnNames.map{ case (baseCol, refCol) => baseCol + "->" + refCol }.mkString(", ")
    val matchPercentage = matchRatio * 100.0
    status match {
      case ConstraintSuccess => s"Key $columnsString can be used for joining. " +
        s"Join columns cardinality in base table: $distinctBefore. " +
        s"Join columns cardinality after joining: $matchingKeys (${"%.2f".format(matchPercentage)}" + "%)."// TODO number format and percentage
      case ConstraintFailure => s"Key $columnsString cannot be used for joining (no result)."
    }
  }
}
