package de.frosner.ddq.reporters

import de.frosner.ddq.check.CheckResult

trait Reporter {

  def report(checkResult: CheckResult): Unit

}
