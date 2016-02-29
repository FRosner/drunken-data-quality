package de.frosner.ddq.constraints

import org.apache.spark.sql.types.DataType

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
