package de.frosner.ddq.constraints

case class DummyConstraintResult(constraint: DummyConstraint,
                                 message: String,
                                 status: ConstraintStatus) extends ConstraintResult[DummyConstraint]
