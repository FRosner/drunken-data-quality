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
