package de.frosner.ddq.constraints

import org.apache.spark.sql.{Column, DataFrame}

import scala.util.Try

case class NeverNullConstraint(columnName: String) extends Constraint {

  val fun = (df: DataFrame) => {
    val tryNullCount = Try(df.filter(new Column(columnName).isNull).count)
    NeverNullConstraintResult(
      constraint = this,
      data = tryNullCount.toOption.map(NeverNullConstraintResultData),
      status = tryNullCount.map(c => if (c == 0) ConstraintSuccess else ConstraintFailure).recoverWith {
        case throwable => Try(ConstraintError(throwable))
      }.get
    )
  }

}
