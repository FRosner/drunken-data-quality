package de.frosner.ddq.constraints

import org.apache.spark.sql.{Column, DataFrame}

import scala.util.Try

case class ExactEqualityConstraint(other: DataFrame) extends Constraint {

  val fun = (df: DataFrame) => {
    val tryEquality = Try {
      if (df.schema != other.schema) {
        throw new IllegalArgumentException("Schemas do not match")
      }
      val dfGroupCount = df.groupBy(df.columns.map(new Column(_)):_*).count()
      val otherGroupCount = other.groupBy(df.columns.map(new Column(_)):_*).count()
      val diffCount1 = dfGroupCount.except(otherGroupCount).count()
      val diffCount2 = otherGroupCount.except(dfGroupCount).count()
      (diffCount1, diffCount2)
    }

    ExactEqualityConstraintResult(
      constraint = this,
      data = tryEquality.toOption.map {
        case (leftToRightCount, rightToLeftCount) => ExactEqualityConstraintData(leftToRightCount, rightToLeftCount)
      },
      status = ConstraintUtil.tryToStatus[(Long, Long)](tryEquality, {
        case (leftToRightCount, rightToLeftCount) => leftToRightCount + rightToLeftCount == 0
      })
    )
  }

}

case class ExactEqualityConstraintResult(constraint: ExactEqualityConstraint,
                                         data: Option[ExactEqualityConstraintData],
                                         status: ConstraintStatus) extends ConstraintResult[ExactEqualityConstraint] {
  val message: String = {
    val otherName = constraint.other.toString()
    val maybeNonMatchingRows = data.map(data => (data.numNonMatchingLeftToRight, data.numNonMatchingRightToLeft))
    val maybePluralS = maybeNonMatchingRows.map {
      case (leftToRightCount, rightToLeftCount) => (
        if (leftToRightCount == 1) "" else "s",
        if (rightToLeftCount == 1) "" else "s"
      )
    }
    val maybeVerb = maybeNonMatchingRows.map {
      case (leftToRightCount, rightToLeftCount) => (
        if (leftToRightCount == 1) "is" else "are",
        if (rightToLeftCount == 1) "is" else "are"
      )
    }
    (status, maybeNonMatchingRows, maybePluralS, maybeVerb) match {
      case (ConstraintSuccess, Some(_), Some(_), Some(_)) =>
        s"It is equal to $otherName."
      case (
        ConstraintFailure,
        Some((leftToRightRows, rightToLeftRows)),
        Some((leftToRightPluralS, rightToLeftPluralS)),
        Some((leftToRightVerb, rightToLeftVerb))
        ) =>
          s"It is not equal ($leftToRightRows distinct count row$leftToRightPluralS $leftToRightVerb " +
            s"present in the checked dataframe but not in the other " +
            s"and $rightToLeftRows distinct count row$rightToLeftPluralS $rightToLeftVerb " +
            s"present in the other dataframe but not in the checked one) to $otherName."
      case (ConstraintError(throwable), None, None, None) =>
        s"Checking equality with $otherName failed: $throwable"
      case default => throw IllegalConstraintResultException(this)
    }
  }
}

case class ExactEqualityConstraintData(numNonMatchingLeftToRight: Long, numNonMatchingRightToLeft: Long)
