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

case class DateFormatConstraintResult(constraint: DateFormatConstraint,
                                      failedRows: Long,
                                      status: ConstraintStatus) extends ConstraintResult[DateFormatConstraint] {
  val message: String = {
    val format = constraint.format.toPattern
    val columnName = constraint.columnName
    val pluralS = if (failedRows == 1) "" else "s"
    val verb = if (failedRows == 1) "is" else "are"
    status match {
      case ConstraintSuccess => s"Column $columnName is formatted by $format."
      case ConstraintFailure => s"Column $columnName contains $failedRows row$pluralS that $verb not formatted by $format."
    }
  }
}

