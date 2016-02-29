package de.frosner.ddq.constraints

import org.apache.spark.sql.{Column, DataFrame}

case class AlwaysNullConstraint(columnName: String) extends Constraint {

  override val fun = (df: DataFrame) => {
    val notNullCount = df.filter(new Column(columnName).isNotNull).count
    AlwaysNullConstraintResult(
      this,
      notNullCount,
      if (notNullCount == 0) ConstraintSuccess else ConstraintFailure
    )
  }

}
