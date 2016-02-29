package de.frosner.ddq.constraints

import org.apache.spark.sql.{Column, DataFrame}

case class ForeignKeyConstraint(columnNames: Seq[(String, String)], referenceTable: DataFrame) extends Constraint {
  val fun = (df: DataFrame) => {
    val renamedColumns = columnNames.map{ case (baseColumn, refColumn) => ("b_" + baseColumn, "r_" + refColumn)}
    val (baseColumns, refColumns) = columnNames.unzip
    val (renamedBaseColumns, renamedRefColumns) = renamedColumns.unzip

    // check if foreign key is a key in reference table
    val nonUniqueRows = referenceTable.groupBy(refColumns.map(new Column(_)):_*).count.filter(new Column("count") > 1).count
    if (nonUniqueRows > 0) {
      ForeignKeyConstraintResult(
        constraint = this,
        numNonMatchingRefs = None,
        status = ConstraintFailure
      )
    } else {
      // rename all columns to avoid ambiguous column references
      val renamedDf = df.select(baseColumns.zip(renamedBaseColumns).map {
        case (original, renamed) => new Column(original).as(renamed)
      }: _*)
      val renamedRef = referenceTable.select(refColumns.zip(renamedRefColumns).map {
        case (original, renamed) => new Column(original).as(renamed)
      }: _*)

      // check if left outer join yields some null values
      val leftOuterJoin = renamedDf.distinct.join(renamedRef, renamedColumns.map {
        case (baseColumn, refColumn) => new Column(baseColumn) === new Column(refColumn)
      }.reduce(_ && _), "outer")
      val notMatchingRefs = leftOuterJoin.filter(renamedRefColumns.map(new Column(_).isNull).reduce(_ && _)).count

      ForeignKeyConstraintResult(
        constraint = this,
        numNonMatchingRefs = Some(notMatchingRefs),
        status = if (notMatchingRefs == 0) ConstraintSuccess else ConstraintFailure
      )
    }
  }
}
