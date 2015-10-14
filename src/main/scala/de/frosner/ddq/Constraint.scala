package de.frosner.ddq

import de.frosner.ddq.Constraint.ConstraintFunction
import org.apache.spark.sql.DataFrame

case class Constraint(fun: ConstraintFunction)

object Constraint {

  type ConstraintFunction = (DataFrame) => ConstraintResult

}
