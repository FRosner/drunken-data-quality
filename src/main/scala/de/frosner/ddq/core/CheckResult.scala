package de.frosner.ddq.core

case class CheckResult(constraintResults: Map[Constraint, ConstraintResult], check: Check, numRows: Long)
