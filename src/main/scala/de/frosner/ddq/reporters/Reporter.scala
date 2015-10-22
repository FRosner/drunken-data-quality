package de.frosner.ddq.reporters

import de.frosner.ddq.core.CheckResult

trait Reporter {

  def report(checkResult: CheckResult): Unit

}
