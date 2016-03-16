package de.frosner.ddq.constraints

import org.apache.spark.sql.{Column, DataFrame}

import scala.util.Try

case class AnyOfConstraint(columnName: String, allowedValues: Set[Any]) extends Constraint {

  val fun = (df: DataFrame) => {
    val maybeError = Try(df.select(new Column(columnName))) // check if column is not ambiguous
    val maybeColumnIndex = maybeError.map(_ => df.columns.indexOf(columnName))
    val maybeNotAllowedCount = maybeColumnIndex.map(columnIndex => df.rdd.filter(row => !row.isNullAt(columnIndex) &&
      !allowedValues.contains(row.get(columnIndex))).count)
    AnyOfConstraintResult(
      constraint = this,
      data = maybeNotAllowedCount.toOption.map(AnyOfConstraintResultData),
      status = maybeNotAllowedCount.map(c => if (c == 0) ConstraintSuccess else ConstraintFailure).recoverWith {
        case throwable => Try(ConstraintError(throwable))
      }.get
    )
  }

}
