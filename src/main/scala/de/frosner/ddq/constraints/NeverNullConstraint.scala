package de.frosner.ddq.constraints

import org.apache.spark.sql.{Column, DataFrame}

case class NeverNullConstraint(columnName: String) extends Constraint {

  val fun = (df: DataFrame) => {
    val nullCount = df.filter(new Column(columnName).isNull).count
    NeverNullConstraintResult(
      this,
      nullCount,
      if (nullCount == 0) ConstraintSuccess else ConstraintFailure
    )
  }

}
