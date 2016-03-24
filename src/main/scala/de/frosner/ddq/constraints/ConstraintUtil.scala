package de.frosner.ddq.constraints

import scala.util.Try

object ConstraintUtil {

  def tryToStatus[T](tryObject: Try[T], successCondition: T => Boolean): ConstraintStatus =
    tryObject.map(
      content => if (successCondition(content)) ConstraintSuccess else ConstraintFailure
    ).recoverWith {
      case throwable => Try(ConstraintError(throwable))
    }.get
}
