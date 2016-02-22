package de.frosner.ddq.constraints

sealed trait ConstraintStatus {
  val stringValue: String
}

object ConstraintSuccess extends ConstraintStatus {
  val stringValue = "Success"
}

object ConstraintFailure extends ConstraintStatus {
  val stringValue = "Failure"
}
