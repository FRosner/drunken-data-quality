package de.frosner.ddq.reporters

import java.io.PrintStream

import de.frosner.ddq.core.CheckResult

abstract class HumanReadableReporter extends Reporter {

  override def report(checkResult: CheckResult): Unit = {
    val check = checkResult.check
    val df = check.dataFrame
    report(
      checkResult = checkResult,
      header = s"Checking ${check.name}",
      prologue = s"It has a total number of ${df.columns.length} columns " +
        s"and ${checkResult.numRows} rows."
    )
  }

  protected def report(checkResult: CheckResult, header: String, prologue: String)

}
