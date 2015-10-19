package de.frosner.ddq.reporters

import java.io.PrintStream

import de.frosner.ddq._
import de.frosner.ddq.check.{ConstraintFailure, ConstraintSuccess, CheckResult}

// TODO give iterable of checks
// TODO document that the stream will not be closed automatically
/**
 * A class which produces a console report of [[CheckResult]].
 *
 * @param stream The [[java.io.PrintStream]] to put the output
**/
case class ConsoleReporter(stream: PrintStream) extends PrintStreamReporter {

  /**
   * Output console report of a given checkResult to the stream passed to the constructor
   * @param checkResult The [[CheckResult]] to be reported
   */
  override def report(checkResult: CheckResult): Unit = {
    stream.println(Console.BLUE + checkResult.header + Console.RESET)
    stream.println(Console.BLUE + checkResult.prologue + Console.RESET)
    if (checkResult.constraintResults.nonEmpty)
      checkResult.constraintResults.foreach {
        case (_, ConstraintSuccess(message)) => stream.println(Console.GREEN + "- " + message + Console.RESET)
        case (_, ConstraintFailure(message)) => stream.println(Console.RED + "- " + message + Console.RESET)
      }
    else
      stream.println(Console.BLUE + "Nothing to check" + Console.RESET)
  }

}
