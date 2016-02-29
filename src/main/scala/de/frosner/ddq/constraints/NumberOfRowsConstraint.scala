package de.frosner.ddq.constraints

import org.apache.spark.sql.DataFrame

case class NumberOfRowsConstraint(expected: Long) extends Constraint {

  val fun = (df: DataFrame) => {
    val count = df.count
    NumberOfRowsConstraintResult(
      constraint = this,
      actual = count,
      status = if (count == expected) ConstraintSuccess else ConstraintFailure
    )
  }

}
