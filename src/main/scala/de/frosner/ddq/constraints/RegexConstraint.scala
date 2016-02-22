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
    val doesNotMatchCount = df.filter(doesNotMatch(new Column(columnName))).count
    RegexConstraintResult(
      constraint = this,
      failedRows = doesNotMatchCount,
      status = if (doesNotMatchCount == 0) ConstraintSuccess else ConstraintFailure
    )
  }

}
