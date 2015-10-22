package de.frosner.ddq.core

import de.frosner.ddq.core.Constraint.ConstraintFunction
import org.apache.spark.sql.DataFrame

case class Constraint(fun: ConstraintFunction)

object Constraint {

  type ConstraintFunction = (DataFrame) => ConstraintResult

}
