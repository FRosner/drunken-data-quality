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

case class FunctionalDependencyConstraintResult(constraint: FunctionalDependencyConstraint,
                                                failedRows: Long,
                                                status: ConstraintStatus) extends ConstraintResult[FunctionalDependencyConstraint] {

  val message: String = {
    val rowPluralS = if (failedRows == 1) "" else "s"
    val dependentSet = constraint.dependentSet
    val determinantString = s"${constraint.determinantSet.mkString(", ")}"
    val dependentString = s"${dependentSet.mkString(", ")}"
    val (columnPluralS, columnVerb) = if (dependentSet.size == 1) ("", "is") else ("s", "are")
    status match {
      case ConstraintSuccess => s"Column$columnPluralS $dependentString $columnVerb functionally dependent on $determinantString."
      case ConstraintFailure => s"Column$columnPluralS $dependentString $columnVerb not functionally dependent on " +
        s"$determinantString ($failedRows violating determinant value$rowPluralS)."
    }
  }

}

