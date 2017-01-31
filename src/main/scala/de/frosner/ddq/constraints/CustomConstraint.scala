package de.frosner.ddq.constraints

import org.apache.spark.sql.DataFrame

import CustomConstraint.{FailureMsg, SuccessMsg}

import scala.util.Try

case class CustomConstraint(name: String,
                            constraintFunction: DataFrame => Either[FailureMsg, SuccessMsg]
                           ) extends Constraint {

  val fun = (df: DataFrame) => {
    val tryFun = Try(constraintFunction(df))
    val messagePrefix = s"Custom constraint '$name'"
    val message = tryFun.map {
      case Left(failureMsg) => s"$messagePrefix failed: $failureMsg"
      case Right(successMsg) => s"$messagePrefix succeeded: $successMsg"
    }.recover {
      case throwable => s"$messagePrefix errored: $throwable"
    }.get
    val status = ConstraintUtil.tryToStatus[Either[FailureMsg, SuccessMsg]](tryFun, _.isRight)
    CustomConstraintResult(this, message, status)
  }

}

case class CustomConstraintResult(constraint: CustomConstraint,
                                  message: String,
                                  status: ConstraintStatus) extends ConstraintResult[CustomConstraint]

object CustomConstraint {

  type SuccessMsg = String
  type FailureMsg = String

}
