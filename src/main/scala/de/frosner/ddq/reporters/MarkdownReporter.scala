package de.frosner.ddq.reporters

import java.io.PrintStream

import de.frosner.ddq.core.CheckResult

/**
 * A class which produces a markdown report of [[CheckResult]].
 *
 * @param stream The [[java.io.PrintStream]] to put the output. The stream will not be closed internally and can
 *               be reused.
 **/
case class MarkdownReporter(stream: PrintStream) extends HumanReadableReporter {

  /**
   * Output markdown report of a given checkResult to the stream passed to the constructor
   * @param checkResult The [[CheckResult]] to be reported
   */
  override def report(checkResult: CheckResult, header: String, prologue: String): Unit = {
    stream.println(s"**$header**\n")
    stream.println(s"$prologue\n")
    if (checkResult.constraintResults.nonEmpty) {
      checkResult.constraintResults.foreach {
        case (_, constraintResult) =>
          stream.println(s"- *${constraintResult.status.stringValue.toUpperCase}*: " + constraintResult.message)
      }
    } else {
      stream.println("Nothing to check!")
    }
    stream.println("")
  }

}
