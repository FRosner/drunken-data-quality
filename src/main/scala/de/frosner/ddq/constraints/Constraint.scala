package de.frosner.ddq.constraints

import de.frosner.ddq.constraints.Constraint.ConstraintFunction
import org.apache.spark.sql.DataFrame

trait Constraint {

  val fun: ConstraintFunction

}

object Constraint {

  type ConstraintFunction = (DataFrame) => ConstraintResult[Constraint]

}
