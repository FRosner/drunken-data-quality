package de.frosner.ddq.core

sealed trait ConstraintResult {

  val message: String

  val status: String

}

case class ConstraintSuccess(message: String) extends ConstraintResult {

  val status = ConstraintSuccess.Status

}

object ConstraintSuccess {

  val Status = "success"

}

case class ConstraintFailure(message: String) extends ConstraintResult {

  val status = ConstraintFailure.Status

}

object ConstraintFailure {

  val Status = "failure"

}

