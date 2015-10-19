package de.frosner.ddq.reporters

import java.io.PrintStream

import de.frosner.ddq._

/**
 * A class which produces a markdown report of [[de.frosner.ddq.CheckResult]].
 *
 * @param stream The [[java.io.PrintStream]] to put the output
 **/
case class MarkdownReporter(stream: PrintStream) extends PrintStreamReporter {

  /**
   * Output markdown report of a given checkResult to the stream passed to the constructor
   * @param checkResult The [[de.frosner.ddq.CheckResult]] to be reported
   */
  override def report(checkResult: CheckResult): Unit = {
    stream.println(s"# ${checkResult.header}\n")
    stream.println(s"${checkResult.prologue}\n")
    if (checkResult.constraintResults.nonEmpty)
      checkResult.constraintResults.foreach {
        case (_, ConstraintSuccess(message)) => stream.println("* [success]: " + message)
        case (_, ConstraintFailure(message)) => stream.println("* [failure]: " + message)
        case (_, Hint(message)) => stream.println("* [hint]: " + message)
      }
    else
      stream.println("Nothing to check")
  }
}
