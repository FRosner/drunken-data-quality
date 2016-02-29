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
