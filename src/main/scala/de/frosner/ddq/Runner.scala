package de.frosner.ddq

import de.frosner.ddq.reporters.Reporter

object Runner {
  def run(checks: Iterable[Check], reporters: Iterable[Reporter]): Iterable[CheckResult] = {
    checks.map(check => {
      val potentiallyPersistedDf = check.cacheMethod.map(check.dataFrame.persist(_)).getOrElse(check.dataFrame)

      val header = s"Checking ${check.displayName.getOrElse(check.dataFrame.toString)}"
      val prologue = s"It has a total number of ${potentiallyPersistedDf.columns.length} columns and ${potentiallyPersistedDf.count} rows."


      val constraintResults = if (check.constraints.nonEmpty)
        check.constraints.map(c => c.fun(potentiallyPersistedDf))
      else
        List(Check.hint("Nothing to check!"))

      val checkResult = CheckResult(header, prologue, constraintResults, check)

      if (check.cacheMethod.isDefined) potentiallyPersistedDf.unpersist()

      reporters.foreach(_.report(checkResult))
      checkResult
    })
  }
}
