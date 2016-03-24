package de.frosner.ddq.core

import de.frosner.ddq.constraints.{Constraint, ConstraintResult}

case class CheckResult(constraintResults: Map[Constraint, ConstraintResult[Constraint]], check: Check, numRows: Long)
