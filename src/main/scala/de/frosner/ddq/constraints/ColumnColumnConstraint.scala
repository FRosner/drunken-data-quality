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
      status = ConstraintUtil.tryToStatus[Long](maybeFailingRows, _ == 0)
    )
  }

}

case class ColumnColumnConstraintResult(constraint: ColumnColumnConstraint,
                                        data: Option[ColumnColumnConstraintResultData],
                                        status: ConstraintStatus) extends ConstraintResult[ColumnColumnConstraint] {

  val message: String = {
    val constraintString = constraint.constraintColumn.toString
    val maybeViolatingRows = data.map(_.failedRows)
    val maybePluralS = maybeViolatingRows.map(violatingRows => if (violatingRows == 1) "" else "s")
    (status, maybeViolatingRows, maybePluralS) match {
      case (ConstraintSuccess, Some(0), _) =>
        s"Constraint $constraintString is satisfied."
      case (ConstraintFailure, Some(violatingRows), Some(pluralS)) =>
        s"$violatingRows row$pluralS did not satisfy constraint $constraintString."
      case (ConstraintError(throwable), None, None) =>
        s"Checking constraint $constraintString failed: $throwable"
      case default => throw IllegalConstraintResultException(this)
    }
  }

}

case class ColumnColumnConstraintResultData(failedRows: Long)
