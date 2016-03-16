package de.frosner.ddq.constraints

import org.apache.spark.sql.{Column, DataFrame}

import scala.util.Try

case class AnyOfConstraint(columnName: String, allowedValues: Set[Any]) extends Constraint {

  val fun = (df: DataFrame) => {
    df.select(new Column(columnName)) // check if reference is not ambiguous
    val columnIndex = df.columns.indexOf(columnName)
    val notAllowedCount = df.rdd.filter(row => !row.isNullAt(columnIndex) &&
      !allowedValues.contains(row.get(columnIndex))).count
    AnyOfConstraintResult(
      constraint = this,
      failedRows = notAllowedCount,
      status = if (notAllowedCount == 0) ConstraintSuccess else ConstraintFailure
    )
  }

}
