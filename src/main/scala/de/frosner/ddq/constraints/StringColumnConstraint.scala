package de.frosner.ddq.constraints

import org.apache.spark.sql.DataFrame

case class StringColumnConstraint(constraintString: String) extends Constraint {

  val fun = (df: DataFrame) => {
    val succeedingRows = df.filter(constraintString).count
    val count = df.count
    val failingRows = count - succeedingRows
    StringColumnConstraintResult(
      constraint = this,
      violatingRows = failingRows,
      if (failingRows == 0) ConstraintSuccess else ConstraintFailure
    )
  }

}

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

