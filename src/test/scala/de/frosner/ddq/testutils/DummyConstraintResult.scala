package de.frosner.ddq.testutils

import de.frosner.ddq.constraints.{ConstraintResult, ConstraintStatus}

case class DummyConstraintResult(constraint: DummyConstraint,
                                 message: String,
                                 status: ConstraintStatus) extends ConstraintResult[DummyConstraint]
