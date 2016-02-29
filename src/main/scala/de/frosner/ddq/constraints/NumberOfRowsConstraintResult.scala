package de.frosner.ddq.constraints

case class NumberOfRowsConstraintResult(constraint: NumberOfRowsConstraint,
                                        actual: Long,
                                        status: ConstraintStatus) extends ConstraintResult[NumberOfRowsConstraint] {

  val message: String = {
    val expected = constraint.expected
    status match {
      case ConstraintSuccess => s"The number of rows is equal to $expected."
      case ConstraintFailure => s"The actual number of rows $actual is not equal to the expected $expected."
    }
  }

}
