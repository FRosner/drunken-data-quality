package de.frosner.ddq.constraints

import java.text.SimpleDateFormat
import java.util.regex.Pattern

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

import scala.util.Try

case class RegexConstraint(columnName: String, regex: String) extends Constraint {

  val fun = (df: DataFrame) => {
    val pattern = Pattern.compile(regex)
    val doesNotMatch = udf((column: String) => column != null && !pattern.matcher(column).find())
    val maybeDoesNotMatchCount = Try(df.filter(doesNotMatch(new Column(columnName))).count)
    RegexConstraintResult(
      constraint = this,
      data = maybeDoesNotMatchCount.toOption.map(RegexConstraintResultData),
      status = ConstraintUtil.tryToStatus[Long](maybeDoesNotMatchCount, _ == 0)
    )
  }

}

case class RegexConstraintResult(constraint: RegexConstraint,
                                 data: Option[RegexConstraintResultData],
                                 status: ConstraintStatus) extends ConstraintResult[RegexConstraint] {

  val message: String = {
    val columnName = constraint.columnName
    val regex = constraint.regex
    val maybeFailedRows = data.map(_.failedRows)
    val maybePluralSAndVerb = maybeFailedRows.map(failedRows => if (failedRows == 1) ("", "does") else ("s", "do"))
    (status, maybeFailedRows, maybePluralSAndVerb) match {
      case (ConstraintSuccess, Some(0), _) =>
        s"Column $columnName matches $regex"
      case (ConstraintFailure, Some(failedRows), Some((pluralS, verb))) =>
        s"Column $columnName contains $failedRows row$pluralS that $verb not match $regex"
      case (ConstraintError(throwable), None, None) =>
        s"Checking whether column $columnName matches $regex failed: $throwable"
    }
  }

}

case class RegexConstraintResultData(failedRows: Long)
