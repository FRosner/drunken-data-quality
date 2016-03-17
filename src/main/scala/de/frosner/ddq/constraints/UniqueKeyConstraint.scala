package de.frosner.ddq.constraints

import org.apache.spark.sql.{DataFrame, Column}

case class UniqueKeyConstraint(columnNames: Seq[String]) extends Constraint {

  require(columnNames.nonEmpty)

  val fun = (df: DataFrame) => {
    val columns = columnNames.map(name => new Column(name))
    val nonUniqueRows = df.groupBy(columns: _*).count.filter(new Column("count") > 1).count
    UniqueKeyConstraintResult(this, nonUniqueRows, if (nonUniqueRows == 0) ConstraintSuccess else ConstraintFailure)
  }

}

case class UniqueKeyConstraintResult(constraint: UniqueKeyConstraint,
                                     numNonUniqueTuples: Long,
                                     status: ConstraintStatus) extends ConstraintResult[UniqueKeyConstraint] {

  val message: String = {
    val columnNames = constraint.columnNames
    val columnsString = columnNames.mkString(", ")
    val isPlural = columnNames.length > 1
    val columnNoun = "Column" + (if (isPlural) "s" else "")
    val columnVerb = if (isPlural) "are" else "is"
    val pluralS = if (numNonUniqueTuples != 1) "s" else ""
    status match {
      case ConstraintSuccess => s"$columnNoun $columnsString $columnVerb a key."
      case ConstraintFailure => s"$columnNoun $columnsString $columnVerb not a key ($numNonUniqueTuples non-unique tuple$pluralS)."
    }
  }

}
