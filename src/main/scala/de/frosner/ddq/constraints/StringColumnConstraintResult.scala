package de.frosner.ddq.constraints

import org.apache.spark.sql.types.DataType

case class StringColumnConstraintResult(constraint: StringColumnConstraint,
                                        violatingRows: Long,
                                        status: ConstraintStatus) extends ConstraintResult[StringColumnConstraint] {

  val message: String = {
    val constraintString = constraint.constraintString
    val pluralS = if (violatingRows == 1) "" else "s"
    status match {
      case ConstraintSuccess => s"Constraint $constraintString is satisfied."
      case ConstraintFailure => s"$violatingRows row$pluralS did not satisfy constraint $constraintString."
    }
  }

}
