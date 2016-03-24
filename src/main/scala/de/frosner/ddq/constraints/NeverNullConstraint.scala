package de.frosner.ddq.constraints

import org.apache.spark.sql.{Column, DataFrame}

import scala.util.Try

case class NeverNullConstraint(columnName: String) extends Constraint {

  val fun = (df: DataFrame) => {
    val tryNullCount = Try(df.filter(new Column(columnName).isNull).count)
    NeverNullConstraintResult(
      constraint = this,
      data = tryNullCount.toOption.map(NeverNullConstraintResultData),
      status = ConstraintUtil.tryToStatus[Long](tryNullCount, _ == 0)
    )
  }

}

case class NeverNullConstraintResult(constraint: NeverNullConstraint,
                                     data: Option[NeverNullConstraintResultData],
                                     status: ConstraintStatus) extends ConstraintResult[NeverNullConstraint] {
  val message: String = {
    val columnName = constraint.columnName
    val maybeNullRows = data.map(_.nullRows)
    val maybePluralS = maybeNullRows.map(nullRows => if (nullRows == 1) "" else "s")
    val maybeVerb = maybeNullRows.map(nullRows => if (nullRows == 1) "is" else "are")
    (status, maybeNullRows, maybePluralS, maybeVerb) match {
      case (ConstraintSuccess, Some(0), Some(pluralS), Some(verb)) =>
        s"Column $columnName is never null."
      case (ConstraintFailure, Some(nullRows), Some(pluralS), Some(verb)) =>
        s"Column $columnName contains $nullRows row$pluralS that $verb null (should never be null)."
      case (ConstraintError(throwable), None, None, None) =>
        s"Checking column $columnName for being never null failed: $throwable"
    }
  }
}

case class NeverNullConstraintResultData(nullRows: Long)
