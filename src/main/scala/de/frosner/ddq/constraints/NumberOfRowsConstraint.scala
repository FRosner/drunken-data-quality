package de.frosner.ddq.constraints

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{count, lit}

case class NumberOfRowsConstraint private[ddq] (expected: Column) extends Constraint {

  val fun = (df: DataFrame) => {
    val countDf = df.agg(count(new Column("*")).as(NumberOfRowsConstraint.countKey))
    val actual = countDf.collect().map(_.getLong(0)).apply(0)
    val satisfied = countDf.select(expected).collect().map(_.getBoolean(0)).apply(0)
    NumberOfRowsConstraintResult(
      constraint = this,
      actual = actual,
      status = if (satisfied) ConstraintSuccess else ConstraintFailure
    )
  }

}

object NumberOfRowsConstraint {

  private[constraints] val countKey: String = "count"

  def apply(expected: Column => Column): NumberOfRowsConstraint = {
    new NumberOfRowsConstraint(expected(new Column(countKey)))
  }

}
