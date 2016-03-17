package de.frosner.ddq.constraints

import org.apache.spark.sql.{Column, DataFrame}

import scala.util.Try

case class ColumnColumnConstraint(constraintColumn: Column) extends Constraint {

  val fun = (df: DataFrame) => {
    val maybeSucceedingRows = Try(df.filter(constraintColumn).count)
    val maybeCount = maybeSucceedingRows.map(_ => df.count)
    val maybeFailingRows = maybeCount.map(count => count - maybeSucceedingRows.get)
    ColumnColumnConstraintResult(
      constraint = this,
      data = maybeFailingRows.toOption.map(ColumnColumnConstraintResultData),
      status = maybeFailingRows.map(c => if (c == 0) ConstraintSuccess else ConstraintFailure).recoverWith {
        case throwable => Try(ConstraintError(throwable))
      }.get
    )
  }

}
