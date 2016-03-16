package de.frosner.ddq.constraints

import org.apache.spark.sql.{Column, DataFrame}

import scala.util.Try

case class AlwaysNullConstraint(columnName: String) extends Constraint {

  override val fun = (df: DataFrame) => {
    val tryNotNullCount = Try(df.filter(new Column(columnName).isNotNull).count)
    AlwaysNullConstraintResult(
      constraint = this,
      status = tryNotNullCount.map(c => if (c == 0) ConstraintSuccess else ConstraintFailure).recoverWith {
        case throwable => Try(ConstraintError(throwable))
      }.get,
      data = tryNotNullCount.toOption.map(AlwaysNullConstraintResultData)
    )
  }

}
