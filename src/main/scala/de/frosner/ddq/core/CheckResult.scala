package de.frosner.ddq.core

case class CheckResult(header: String,
                       prologue: String,
                       constraintResults: Map[Constraint, ConstraintResult],
                       check: Check)
