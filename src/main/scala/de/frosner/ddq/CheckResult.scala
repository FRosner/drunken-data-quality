package de.frosner.ddq


case class CheckResult(header: String, prologue: String, constraintResults: Map[Constraint, ConstraintResult],
                       check:Check)
