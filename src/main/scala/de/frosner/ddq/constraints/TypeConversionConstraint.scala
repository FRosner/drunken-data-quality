package de.frosner.ddq.constraints

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, DataFrame}

import scala.util.Try

case class TypeConversionConstraint(columnName: String,
                                    convertedType: DataType) extends Constraint {

  val fun = (df: DataFrame) => {
    val originalColumn = new Column(columnName)
    val castedColumnName = columnName + "_casted"
    val maybeCasted = Try(df.select(originalColumn, originalColumn.cast(convertedType).as(castedColumnName)))
    val maybeFailedCastsAndOriginalType = maybeCasted.map(casted => {
      val failedCastsCount = casted.filter(new Column(castedColumnName).isNull && originalColumn.isNotNull).count
      val originalType = df.schema.find(_.name == columnName).get.dataType
      (failedCastsCount, originalType)
    })
    TypeConversionConstraintResult(
      constraint = this,
      data = maybeFailedCastsAndOriginalType.toOption.map{ case (failedCastsCount, originalType) =>
        TypeConversionConstraintResultData(
          originalType = originalType,
          failedRows = failedCastsCount
        )
      },
      status = ConstraintUtil.tryToStatus[Long](maybeFailedCastsAndOriginalType.map{
        case (failedCastsCount, originalType) => failedCastsCount
      }, _ == 0)
    )
  }

}

case class TypeConversionConstraintResult(constraint: TypeConversionConstraint,
                                          data: Option[TypeConversionConstraintResultData],
                                          status: ConstraintStatus) extends ConstraintResult[TypeConversionConstraint] {

  val message: String = {
    val convertedType = constraint.convertedType
    val columnName = constraint.columnName
    val maybePluralSVerb = data.map(data => if (data.failedRows == 1) ("", "is") else ("s", "are"))
    (status, data, maybePluralSVerb) match {
      case (ConstraintSuccess, Some(TypeConversionConstraintResultData(originalType, 0)), _) =>
        s"Column $columnName can be converted from $originalType to $convertedType."
      case (ConstraintFailure, Some(TypeConversionConstraintResultData(originalType, failedRows)), Some((pluralS, verb))) =>
        s"Column $columnName cannot be converted from $originalType to $convertedType. " +
        s"$failedRows row$pluralS could not be converted."
      case (ConstraintError(throwable), None, None) =>
        s"Checking whether column $columnName can be converted to $convertedType failed: $throwable"
      case default => throw IllegalConstraintResultException(this)
    }
  }

}

case class TypeConversionConstraintResultData(originalType: DataType, failedRows: Long)
