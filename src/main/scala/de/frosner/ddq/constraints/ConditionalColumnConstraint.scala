package de.frosner.ddq.constraints

import org.apache.spark.sql.{Column, DataFrame}

import scala.util.Try

case class ConditionalColumnConstraint(statement: Column, implication: Column) extends Constraint {

  val fun = (df: DataFrame) => {
    val maybeFailingRows = Try {
      val succeedingRows = df.filter(!statement || implication).count
      df.count - succeedingRows
    }
    ConditionalColumnConstraintResult(
      constraint = this,
      data = maybeFailingRows.toOption.map(ConditionalColumnConstraintResultData),
      status = ConstraintUtil.tryToStatus[Long](maybeFailingRows, _ == 0)
    )
  }

}

case class ConditionalColumnConstraintResult(constraint: ConditionalColumnConstraint,
                                             data: Option[ConditionalColumnConstraintResultData],
                                             status: ConstraintStatus) extends ConstraintResult[ConditionalColumnConstraint] {

  val message: String = ColumnConstraintUtil.createColumnConstraintMessage(
    status = status,
    constraintResult = this,
    constraintString = s"${constraint.statement} -> ${constraint.implication}",
    maybeViolatingRows = data.map(_.failedRows)
  )

}

case class ConditionalColumnConstraintResultData(failedRows: Long)
