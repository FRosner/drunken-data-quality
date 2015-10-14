package de.frosner.ddq


case class CheckResult(header: String, prologue: String, constraintResults: Iterable[ConstraintResult], check:Check)
