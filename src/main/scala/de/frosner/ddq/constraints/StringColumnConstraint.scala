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

  val message: String = ColumnConstraintUtil.createColumnConstraintMessage(
    status = status,
    constraintResult = this,
    constraintString = constraint.constraintString,
    maybeViolatingRows = data.map(_.failedRows)
  )

}

case class StringColumnConstraintResultData(failedRows: Long)

