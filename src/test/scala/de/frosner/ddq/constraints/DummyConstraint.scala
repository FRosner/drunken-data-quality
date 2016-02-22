package de.frosner.ddq.constraints

import org.apache.spark.sql.DataFrame

case class DummyConstraint(message: String, status: ConstraintStatus) extends Constraint {

  override val fun = (df: DataFrame) => DummyConstraintResult(this, message, status)

}
