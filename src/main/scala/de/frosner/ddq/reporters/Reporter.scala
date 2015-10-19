package de.frosner.ddq.reporters

import de.frosner.ddq.CheckResult

trait Reporter {

  def report(checkResult: CheckResult): Unit

}