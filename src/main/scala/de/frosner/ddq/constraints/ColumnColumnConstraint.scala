package de.frosner.ddq.constraints

import org.apache.spark.sql.{Column, DataFrame}

case class ColumnColumnConstraint(constraintColumn: Column) extends Constraint {

  val fun = (df: DataFrame) => {
    val succeedingRows = df.filter(constraintColumn).count
    val count = df.count
    val failingRows = count - succeedingRows
    ColumnColumnConstraintResult(
      constraint = this,
      violatingRows = failingRows,
      if (failingRows == 0) ConstraintSuccess else ConstraintFailure
    )
  }

}
