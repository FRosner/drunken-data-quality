package de.frosner.ddq.constraints

import org.apache.spark.sql.{Column, DataFrame}

case class ConditionalColumnConstraint(statement: Column, implication: Column) extends Constraint {

  val fun = (df: DataFrame) => {
    val succeedingRows = df.filter(!statement || implication).count
    val count = df.count
    val failingRows = count - succeedingRows
    ConditionalColumnConstraintResult(
      constraint = this,
      violatingRows = failingRows,
      if (failingRows == 0) ConstraintSuccess else ConstraintFailure
    )
  }

}

case class ConditionalColumnConstraintResult(constraint: ConditionalColumnConstraint,
                                             violatingRows: Long,
                                             status: ConstraintStatus) extends ConstraintResult[ConditionalColumnConstraint] {

  val message: String = {
    val constraintString = s"${constraint.statement} -> ${constraint.implication}"
    val pluralS = if (violatingRows == 1) "" else "s"
    status match {
      case ConstraintSuccess => s"Constraint $constraintString is satisfied."
      case ConstraintFailure => s"$violatingRows row$pluralS did not satisfy constraint $constraintString."
    }
  }

}
