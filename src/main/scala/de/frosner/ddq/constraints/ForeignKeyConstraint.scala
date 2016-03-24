package de.frosner.ddq.constraints

import org.apache.spark.sql.{Column, DataFrame}

import scala.util.Try

case class ForeignKeyConstraint(columnNames: Seq[(String, String)], referenceTable: DataFrame) extends Constraint {

  val fun = (df: DataFrame) => {
    val renamedColumns = columnNames.map{ case (baseColumn, refColumn) => ("b_" + baseColumn, "r_" + refColumn)}
    val (baseColumns, refColumns) = columnNames.unzip
    val (renamedBaseColumns, renamedRefColumns) = renamedColumns.unzip

    // check if foreign key is a key in reference table
    val maybeNonUniqueRows = Try(
      referenceTable.groupBy(refColumns.map(new Column(_)):_*).count.filter(new Column("count") > 1).count
    )
    if (maybeNonUniqueRows.toOption.exists(_ > 0)) {
      ForeignKeyConstraintResult(
        constraint = this,
        data = Some(ForeignKeyConstraintResultData(numNonMatchingRefs = None)),
        status = ConstraintFailure
      )
    } else {
      // rename all columns to avoid ambiguous column references
      val maybeRenamedDfAndRef = maybeNonUniqueRows.map(_ => {
        val renamedDf = df.select(baseColumns.zip(renamedBaseColumns).map {
          case (original, renamed) => new Column(original).as(renamed)
        }: _*)
        val renamedRef = referenceTable.select(refColumns.zip(renamedRefColumns).map {
          case (original, renamed) => new Column(original).as(renamed)
        }: _*)
        (renamedDf, renamedRef)
      }
    )


      // check if left outer join yields some null values
      val maybeLeftOuterJoin = maybeRenamedDfAndRef.map { case (renamedDf, renamedRef) =>
        val joinCondition = renamedColumns.map {
          case (baseColumn, refColumn) => new Column(baseColumn) === new Column(refColumn)
        }.reduce(_ && _)
        renamedDf.distinct.join(renamedRef, joinCondition, "outer")
      }

      val maybeNotMatchingRefs = maybeLeftOuterJoin.map(_.filter(renamedRefColumns.map(new Column(_).isNull).reduce(_ && _)).count)

      ForeignKeyConstraintResult(
        constraint = this,
        data = maybeNotMatchingRefs.toOption.map(Some(_)).map(ForeignKeyConstraintResultData),
        status = ConstraintUtil.tryToStatus[Long](maybeNotMatchingRefs, _ == 0)
      )
    }
  }

}

case class ForeignKeyConstraintResult(constraint: ForeignKeyConstraint,
                                      data: Option[ForeignKeyConstraintResultData],
                                      status: ConstraintStatus) extends ConstraintResult[ForeignKeyConstraint] {

  val message: String = {
    val referenceTable = constraint.referenceTable
    val columnNames = constraint.columnNames
    val columnsString = columnNames.map { case (baseCol, refCol) => baseCol + "->" + refCol }.mkString(", ")
    val isPlural = columnNames.length > 1
    val (columnDo, columnDefine, columnIs, columnPluralS) =
      if (isPlural) ("do", "define", "are", "s") else ("does", "defines", "is", "")
    val columnNoun = "Column" + columnPluralS
    val maybeNumNonMatchingRefs = data.map(_.numNonMatchingRefs)
    (status, maybeNumNonMatchingRefs) match {
      case (ConstraintSuccess, Some(Some(0))) =>
        s"$columnNoun $columnsString $columnDefine a foreign key " +
        s"pointing to the reference table $referenceTable."
      case (ConstraintFailure, Some(None)) =>
        s"$columnNoun $columnsString $columnIs not a key in the reference table."
      case (ConstraintFailure, Some(Some(nonMatching))) =>
        val (rowsNoun, rowsDo) = if (nonMatching != 1) ("rows", "do") else ("row", "does")
        s"$columnNoun $columnsString $columnDo not define a foreign key " +
          s"pointing to $referenceTable. $nonMatching $rowsNoun $rowsDo not match."
      case (ConstraintError(throwable), None) =>
        s"Checking whether ${columnNoun.toLowerCase} $columnsString $columnDefine a foreign key failed: $throwable"
      case default => throw IllegalConstraintResultException(this)
    }
  }

}

case class ForeignKeyConstraintResultData(numNonMatchingRefs: Option[Long])
