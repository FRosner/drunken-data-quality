package de.frosner.ddq.check

case class CheckResult(header: String, prologue: String, constraintResults: Map[Constraint, ConstraintResult],
                       check:Check)
