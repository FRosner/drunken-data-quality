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
