package de.frosner.ddq.constraints

trait ConstraintResult[+C <: Constraint] {
  val constraint: C
  val status: ConstraintStatus
  val message: String
}
