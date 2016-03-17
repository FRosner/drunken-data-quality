package de.frosner.ddq.constraints

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, DataFrame}

case class TypeConversionConstraint(columnName: String,
                                    convertedType: DataType) extends Constraint {

  val fun = (df: DataFrame) => {
    val originalColumn = new Column(columnName)
    val castedColumnName = columnName + "_casted"
    val casted = df.select(originalColumn, originalColumn.cast(convertedType).as(castedColumnName))
    val failedCastsCount = casted.filter(new Column(castedColumnName).isNull && originalColumn.isNotNull).count
    val originalType = df.schema.find(_.name == columnName).get.dataType
    TypeConversionConstraintResult(
      this,
      originalType,
      failedCastsCount,
      if (failedCastsCount == 0) ConstraintSuccess else ConstraintFailure
    )
  }

}

case class TypeConversionConstraintResult(constraint: TypeConversionConstraint,
                                          originalType: DataType,
                                          failedRows: Long,
                                          status: ConstraintStatus) extends ConstraintResult[TypeConversionConstraint] {
  val message: String = {
    val convertedType = constraint.convertedType
    val columnName = constraint.columnName
    val pluralS = if (failedRows == 1) "" else "s"
    val verb = if (failedRows == 1) "is" else "are"
    status match {
      case ConstraintSuccess => s"Column $columnName can be converted from $originalType to $convertedType."
      case ConstraintFailure => s"Column $columnName cannot be converted from $originalType to $convertedType. " +
        s"$failedRows row$pluralS could not be converted."
    }
  }

}
