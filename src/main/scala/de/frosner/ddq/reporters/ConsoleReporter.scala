package de.frosner.ddq.reporters

import java.io.PrintStream

import de.frosner.ddq._

// TODO give iterable of checks
// TODO document that the stream will not be closed automatically
case class ConsoleReporter(stream: PrintStream) extends PrintStreamReporter {

  override def report(checkResult: CheckResult): Unit = {
    stream.println(Console.BLUE + checkResult.header + Console.RESET)
    stream.println(Console.BLUE + checkResult.prologue + Console.RESET)
    checkResult.constraintResults.foreach {
      case ConstraintSuccess(message) => stream.println(Console.GREEN + "- " + message + Console.RESET)
      case ConstraintFailure(message) => stream.println(Console.RED + "- " + message + Console.RESET)
      case Hint(message) => stream.println(Console.BLUE + message + Console.RESET)
    }
  }

}