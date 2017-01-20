package de.frosner.ddq.constraints

import org.apache.spark.sql.{Column, DataFrame}

import scala.util.Try

case class AlwaysNullConstraint(columnName: String) extends Constraint {

  override val fun = (df: DataFrame) => {
    val tryNotNullCountAndNulls = Try {
      val originalColumns = df.columns
      val withNotNullColumn = df.withColumn(uuid, new Column(columnName).isNotNull)
      val notNulls = withNotNullColumn.filter(new Column(uuid)).select(originalColumns.map(new Column(_)): _*)
      val notNullCount = notNulls.count
      (notNullCount, notNulls)
    }
    val tryNotNullCount = tryNotNullCountAndNulls.map {
      case (notNullCount, notNulls) => notNullCount
    }
    AlwaysNullConstraintResult(
      constraint = this,
      status = ConstraintUtil.tryToStatus[Long](tryNotNullCount, _ == 0),
      data = tryNotNullCountAndNulls.toOption.map {
        case (notNullCount, notNulls) => AlwaysNullConstraintResultData(
          nonNullRows = notNullCount,
          notNulls = notNulls
        )
      }
    )
  }

}

case class AlwaysNullConstraintResult(constraint: AlwaysNullConstraint,
                                      status: ConstraintStatus,
                                      data: Option[AlwaysNullConstraintResultData]
                                     ) extends ConstraintResult[AlwaysNullConstraint] {

  val message: String = {
    val columnName = constraint.columnName
    val maybeNonNullRows = data.map(_.nonNullRows)
    val maybePluralS = maybeNonNullRows.map(n => if (n == 1) "" else "s")
    (status, maybeNonNullRows, maybePluralS) match {
      case (ConstraintError(throwable), None, None) =>
        s"Checking column $columnName for being always null failed: $throwable"
      case (ConstraintSuccess, Some(0), Some(pluralS)) =>
        s"Column $columnName is always null."
      case (ConstraintFailure, Some(nonNullRows), Some(pluralS)) =>
        s"Column $columnName contains $nonNullRows non-null row$pluralS (should always be null)."
      case default => throw IllegalConstraintResultException(this)
    }
  }

}

case class AlwaysNullConstraintResultData(nonNullRows: Long, notNulls: DataFrame)
