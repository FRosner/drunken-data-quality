package de.frosner.ddq.constraints

import java.util.regex.Pattern

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

case class FunctionalDependencyConstraint(determinantSet: Seq[String],
                                          dependentSet: Seq[String]) extends Constraint {

  require(determinantSet.nonEmpty, "determinantSet must not be empty")
  require(dependentSet.nonEmpty, "dependentSet must not be empty")

  val fun = (df: DataFrame) => {
    val determinantColumns = determinantSet.map(columnName => new Column(columnName))
    val dependentColumns = dependentSet.map(columnName => new Column(columnName))
    val relevantSelection = df.select(determinantColumns ++ dependentColumns: _*)

    val determinantValueCounts = relevantSelection.distinct.groupBy(determinantColumns: _*).count
    val violatingDeterminantValuesCount = determinantValueCounts.filter(new Column("count") !== 1).count
    FunctionalDependencyConstraintResult(
      constraint = this,
      failedRows = violatingDeterminantValuesCount,
      if (violatingDeterminantValuesCount == 0) ConstraintSuccess else ConstraintFailure
    )
  }

}
