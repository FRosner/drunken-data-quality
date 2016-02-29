package de.frosner.ddq.constraints

import org.apache.spark.sql.{Column, DataFrame}

case class ConditionalColumnConstraint(statement: Column, implication: Column) extends Constraint {

  val fun = (df: DataFrame) => {
    val succeedingRows = df.filter(!statement || implication).count
    val count = df.count
    val failingRows = count - succeedingRows
    ConditionalColumnConstraintResult(
      constraint = this,
      violatingRows = failingRows,
      if (failingRows == 0) ConstraintSuccess else ConstraintFailure
    )
  }

}
