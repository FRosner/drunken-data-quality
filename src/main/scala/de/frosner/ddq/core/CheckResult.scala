package de.frosner.ddq.core

import de.frosner.ddq.constraints.{ConstraintResult, Constraint}

case class CheckResult(constraintResults: Map[Constraint, ConstraintResult[Constraint]], check: Check, numRows: Long)
