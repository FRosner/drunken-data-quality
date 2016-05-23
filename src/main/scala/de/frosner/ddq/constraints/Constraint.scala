package de.frosner.ddq.constraints

import java.util.UUID

import de.frosner.ddq.constraints.Constraint.ConstraintFunction
import org.apache.spark.sql.DataFrame

trait Constraint {

  val fun: ConstraintFunction

  val uuid: String = UUID.randomUUID().toString

}

object Constraint {

  type ConstraintFunction = (DataFrame) => ConstraintResult[Constraint]

}
