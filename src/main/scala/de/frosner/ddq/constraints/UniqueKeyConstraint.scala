package de.frosner.ddq.constraints

import org.apache.spark.sql.{DataFrame, Column}

import scala.util.Try

case class UniqueKeyConstraint(columnNames: Seq[String]) extends Constraint {

  require(columnNames.nonEmpty)

  val fun = (df: DataFrame) => {
    val columns = columnNames.map(name => new Column(name))
    val maybeNonUniqueRows = Try(df.groupBy(columns: _*).count.filter(new Column("count") > 1).count)
    UniqueKeyConstraintResult(
      constraint = this,
      data = maybeNonUniqueRows.toOption.map(UniqueKeyConstraintResultData),
      status = ConstraintUtil.tryToStatus[Long](maybeNonUniqueRows, _ == 0)
    )
  }

}

case class UniqueKeyConstraintResult(constraint: UniqueKeyConstraint,
                                     data: Option[UniqueKeyConstraintResultData],
                                     status: ConstraintStatus) extends ConstraintResult[UniqueKeyConstraint] {

  val message: String = {
    val columnNames = constraint.columnNames
    val columnsString = columnNames.mkString(", ")
    val isPlural = columnNames.length > 1
    val columnNoun = "Column" + (if (isPlural) "s" else "")
    val columnVerb = if (isPlural) "are" else "is"
    val maybeNumNonUniqueTuples = data.map(_.numNonUniqueTuples)
    val maybePluralS = maybeNumNonUniqueTuples.map(numNonUniqueTuples => if (numNonUniqueTuples != 1) "s" else "")
    (status, maybeNumNonUniqueTuples, maybePluralS) match {
      case (ConstraintSuccess, Some(0), _) =>
        s"$columnNoun $columnsString $columnVerb a key."
      case (ConstraintFailure, Some(numNonUniqueTuples), Some(pluralS)) =>
        s"$columnNoun $columnsString $columnVerb not a key ($numNonUniqueTuples non-unique tuple$pluralS)."
      case (ConstraintError(throwable), None, None) =>
        s"Checking whether ${columnNoun.toLowerCase()} $columnsString $columnVerb a key failed: $throwable"
    }
  }

}

case class UniqueKeyConstraintResultData(numNonUniqueTuples: Long)
