package de.frosner.ddq.reporters

import java.io.PrintStream

import de.frosner.ddq._

case class MarkdownReporter(stream: PrintStream) extends PrintStreamReporter {
  override def report(checkResult: CheckResult): Unit = {
    stream.println(s"# ${checkResult.header}\n")
    stream.println(s"${checkResult.prologue}\n")
    checkResult.constraintResults.foreach {
      case ConstraintSuccess(message) => stream.println("* [success]: " + message)
      case ConstraintFailure(message) => stream.println("* [failure]: " + message)
      case Hint(message) => stream.println("* [hint]: " + message)
    }
  }
}
