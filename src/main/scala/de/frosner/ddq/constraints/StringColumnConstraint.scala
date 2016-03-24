package de.frosner.ddq.constraints

import org.apache.spark.sql.DataFrame

import scala.util.Try

case class StringColumnConstraint(constraintString: String) extends Constraint {

  val fun = (df: DataFrame) => {
    val maybeSucceedingRows = Try(df.filter(constraintString).count)
    val count = df.count
    val maybeFailingRows = maybeSucceedingRows.map(succeedingRows => count - succeedingRows)
    StringColumnConstraintResult(
      constraint = this,
      data = maybeFailingRows.toOption.map(StringColumnConstraintResultData),
      status = ConstraintUtil.tryToStatus[Long](maybeFailingRows, _ == 0)
    )
  }

}

case class StringColumnConstraintResult(constraint: StringColumnConstraint,
                                        data: Option[StringColumnConstraintResultData],
                                        status: ConstraintStatus) extends ConstraintResult[StringColumnConstraint] {

  val message: String = {
    val constraintString = constraint.constraintString
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

case class StringColumnConstraintResultData(failedRows: Long)

