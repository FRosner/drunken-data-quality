package de.frosner.ddq

import de.frosner.ddq.reporters.Reporter


/**
 * An object responsible for running checks and producing reports
 */
object Runner {
  /**
   * Run checks and then report to the reporters. Each check will be reported by every reporter.
   *
   * @param checks An iterable of Check objects to be reported
   * @param reporters An iterable of Reporters
   * @return CheckResult for every check passed as an argument
   */
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
