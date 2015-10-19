package de.frosner.ddq.check

import de.frosner.ddq.check.Constraint.ConstraintFunction
import org.apache.spark.sql.DataFrame

case class Constraint(fun: ConstraintFunction)

object Constraint {

  type ConstraintFunction = (DataFrame) => ConstraintResult

}
