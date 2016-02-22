package de.frosner.ddq.constraints

import java.text.SimpleDateFormat

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

import scala.util.Try

case class DateFormatConstraint(columnName: String,
                                format: SimpleDateFormat) extends Constraint {

  val fun = (df: DataFrame) => {
    val cannotBeDate = udf((column: String) => column != null && Try(format.parse(column)).isFailure)
    val cannotBeDateCount = df.filter(cannotBeDate(new Column(columnName))).count
    DateFormatConstraintResult(
      this,
      cannotBeDateCount,
      if (cannotBeDateCount == 0) ConstraintSuccess else ConstraintFailure
    )
  }

}
