package de.frosner.ddq.constraints

import org.apache.spark.sql.{Column, DataFrame}

import scala.util.Try

case class JoinableConstraint(columnNames: Seq[(String, String)], referenceTable: DataFrame) extends Constraint {

  val fun = (df: DataFrame) => {
    val columnsMap = columnNames.toMap
    val renamedColumns = columnNames.map{ case (baseColumn, refColumn) => ("b_" + baseColumn, "r_" + refColumn)}
    val (baseColumns, refColumns) = columnNames.unzip
    val (renamedBaseColumns, renamedRefColumns) = renamedColumns.unzip

    val maybeNonUniqueRows = Try(
      referenceTable.groupBy(refColumns.map(new Column(_)):_*).count.filter(new Column("count") > 1).count
    )

    // rename all columns to avoid ambiguous column references
    val maybeRenamedDfAndRef = maybeNonUniqueRows.map(_ => {
      val renamedDf = df.select(baseColumns.zip(renamedBaseColumns).map {
        case (original, renamed) => new Column(original).as(renamed)
      }: _*)
      val renamedRef = referenceTable.select(refColumns.zip(renamedRefColumns).map {
        case (original, renamed) => new Column(original).as(renamed)
      }: _*)
      (renamedDf, renamedRef)
    })

    // check if join yields some values
    val maybeDistinctBeforeAndMatchingRows = maybeRenamedDfAndRef.map { case (renamedDf, renamedRef) =>
      val renamedDfDistinct = renamedDf.distinct
      val distinctBefore = renamedDfDistinct.count
      val joinCondition = renamedColumns.map{
        case (baseColumn, refColumn) => new Column(baseColumn) === new Column(refColumn)
      }.reduce(_ && _)
      val join = renamedDfDistinct.join(renamedRef, joinCondition)
      val matchingRows = join.distinct.count
      (distinctBefore, matchingRows)
    }

    JoinableConstraintResult(
      constraint = this,
      data = maybeDistinctBeforeAndMatchingRows.toOption.map{ case (distinctBefore, matchingRows) =>
        JoinableConstraintResultData(
          distinctBefore = distinctBefore,
          matchingKeys = matchingRows
        )
      },
      status = ConstraintUtil.tryToStatus[Long](maybeDistinctBeforeAndMatchingRows.map{
        case (distinctBefore, matchingRows) => matchingRows
      }, _ > 0)
    )
  }

}

case class JoinableConstraintResult(constraint: JoinableConstraint,
                                    data: Option[JoinableConstraintResultData],
                                    status: ConstraintStatus) extends ConstraintResult[JoinableConstraint] {

  val maybeMatchRatio: Option[Double] = data.map(d => d.matchingKeys.toDouble / d.distinctBefore)

  val message: String = {
    val columnNames = constraint.columnNames
    val columnsString = columnNames.map{ case (baseCol, refCol) => baseCol + "->" + refCol }.mkString(", ")
    val maybeMatchPercentage = maybeMatchRatio.map(_ * 100.0)
    (status, data, maybeMatchPercentage) match {
      case (ConstraintSuccess, Some(JoinableConstraintResultData(distinctBefore, matchingKeys)), Some(matchPercentage)) =>
        s"Key $columnsString can be used for joining. " +
        s"Join columns cardinality in base table: $distinctBefore. " +
        s"Join columns cardinality after joining: $matchingKeys (${"%.2f".format(matchPercentage)}" + "%)."
      case (ConstraintFailure, Some(_), Some(_)) => s"Key $columnsString cannot be used for joining (no result)."
      case (ConstraintError(throwable), None, None) =>
        s"Checking whether $columnsString can be used for joining failed: $throwable"
      case default => throw IllegalConstraintResultException(this)
    }
  }

}

case class JoinableConstraintResultData(distinctBefore: Long, matchingKeys: Long)
