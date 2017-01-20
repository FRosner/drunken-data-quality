package de.frosner.ddq.constraints

import java.text.SimpleDateFormat

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

import scala.util.Try

case class DateFormatConstraint(columnName: String,
                                formatString: String) extends Constraint {

  val fun = (df: DataFrame) => {
    val cannotBeDate = udf((column: String) => column != null && Try(new SimpleDateFormat(formatString).parse(column)).isFailure)
    val maybeCannotBeDateCount = Try(df.filter(cannotBeDate(new Column(columnName))).count)
    DateFormatConstraintResult(
      this,
      data = maybeCannotBeDateCount.toOption.map(DateFormatConstraintResultData),
      status = ConstraintUtil.tryToStatus[Long](maybeCannotBeDateCount, _ == 0)
    )
  }

}

case class DateFormatConstraintResult(constraint: DateFormatConstraint,
                                      data: Option[DateFormatConstraintResultData],
                                      status: ConstraintStatus) extends ConstraintResult[DateFormatConstraint] {

  val message: String = {
    val format = constraint.formatString
    val columnName = constraint.columnName
    val maybeFailedRows = data.map(_.failedRows)
    val maybePluralS = maybeFailedRows.map(failedRows => if (failedRows == 1) "" else "s")
    val maybeVerb = maybeFailedRows.map(failedRows => if (failedRows == 1) "is" else "are")
    (status, maybeFailedRows, maybePluralS, maybeVerb) match {
      case (ConstraintSuccess, Some(0), _, _) =>
        s"Column $columnName is formatted by $format."
      case (ConstraintFailure, Some(failedRows), Some(pluralS), Some(verb)) =>
        s"Column $columnName contains $failedRows row$pluralS that $verb not formatted by $format."
      case (ConstraintError(throwable), None, None, None) =>
        s"Checking whether column $columnName is formatted by $format failed: $throwable"
      case default => throw IllegalConstraintResultException(this)
    }

  }

}

case class DateFormatConstraintResultData(failedRows: Long)
