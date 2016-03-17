package de.frosner.ddq.constraints

import org.apache.spark.sql.{Column, DataFrame}

case class JoinableConstraint(columnNames: Seq[(String, String)], referenceTable: DataFrame) extends Constraint {

  val fun = (df: DataFrame) => {
    val columnsMap = columnNames.toMap
    val renamedColumns = columnNames.map{ case (baseColumn, refColumn) => ("b_" + baseColumn, "r_" + refColumn)}
    val (baseColumns, refColumns) = columnNames.unzip
    val (renamedBaseColumns, renamedRefColumns) = renamedColumns.unzip

    val nonUniqueRows = referenceTable.groupBy(refColumns.map(new Column(_)):_*).count.filter(new Column("count") > 1).count

    // rename all columns to avoid ambiguous column references
    val renamedDf = df.select(baseColumns.zip(renamedBaseColumns).map {
      case (original, renamed) => new Column(original).as(renamed)
    }:_*)
    val renamedRef = referenceTable.select(refColumns.zip(renamedRefColumns).map {
      case (original, renamed) => new Column(original).as(renamed)
    }:_*)

    // check if join yields some values
    val renamedDfDistinct = renamedDf.distinct
    val distinctBefore = renamedDfDistinct.count
    val join = renamedDfDistinct.join(renamedRef, renamedColumns.map{
      case (baseColumn, refColumn) => new Column(baseColumn) === new Column(refColumn)
    }.reduce(_ && _))
    val matchingRows = join.distinct.count
    val matchedKeysPercentage = ((matchingRows.toDouble / distinctBefore) * 100).round

    JoinableConstraintResult(
      constraint = this,
      distinctBefore = distinctBefore,
      matchingKeys = matchingRows,
      status = if (matchingRows > 0) ConstraintSuccess else ConstraintFailure
    )
  }

}

case class JoinableConstraintResult(constraint: JoinableConstraint,
                                    distinctBefore: Long,
                                    matchingKeys: Long,
                                    status: ConstraintStatus) extends ConstraintResult[JoinableConstraint] {

  val matchRatio: Double = matchingKeys.toDouble / distinctBefore

  val message: String = {
    val columnNames = constraint.columnNames
    val columnsString = columnNames.map{ case (baseCol, refCol) => baseCol + "->" + refCol }.mkString(", ")
    val matchPercentage = matchRatio * 100.0
    status match {
      case ConstraintSuccess => s"Key $columnsString can be used for joining. " +
        s"Join columns cardinality in base table: $distinctBefore. " +
        s"Join columns cardinality after joining: $matchingKeys (${"%.2f".format(matchPercentage)}" + "%)."
      case ConstraintFailure => s"Key $columnsString cannot be used for joining (no result)."
    }
  }
}
