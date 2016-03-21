package de.frosner.ddq.constraints

import org.apache.spark.sql.{Column, DataFrame}

import scala.util.Try

case class ConditionalColumnConstraint(statement: Column, implication: Column) extends Constraint {

  val fun = (df: DataFrame) => {
    val maybeSucceedingRows = Try(df.filter(!statement || implication).count)
    val maybeCount = maybeSucceedingRows.map(_ => df.count)
    val maybeFailingRows = maybeCount.map(count => count - maybeSucceedingRows.get)
    ConditionalColumnConstraintResult(
      constraint = this,
      data = maybeFailingRows.toOption.map(ConditionalColumnConstraintResultData),
      status = maybeFailingRows.map(failingRows => if (failingRows == 0) ConstraintSuccess else ConstraintFailure).recoverWith {
        case throwable => Try(ConstraintError(throwable))
      }.get
    )
  }

}

case class ConditionalColumnConstraintResult(constraint: ConditionalColumnConstraint,
                                             data: Option[ConditionalColumnConstraintResultData],
                                             status: ConstraintStatus) extends ConstraintResult[ConditionalColumnConstraint] {

  val message: String = {
    val constraintString = s"${constraint.statement} -> ${constraint.implication}"
    val maybeViolatingRows = data.map(_.failedRows)
    val maybePluralS = maybeViolatingRows.map(violatingRows => if (violatingRows == 1) "" else "s")
    (status, maybeViolatingRows, maybePluralS) match {
      case (ConstraintSuccess, Some(0), _) =>
        s"Constraint $constraintString is satisfied."
      case (ConstraintFailure, Some(violatingRows), Some(pluralS)) =>
        s"$violatingRows row$pluralS did not satisfy constraint $constraintString."
      case (ConstraintError(throwable), None, None) =>
        s"Checking constraint $constraintString failed: $throwable"
    }
  }

}

case class ConditionalColumnConstraintResultData(failedRows: Long)
