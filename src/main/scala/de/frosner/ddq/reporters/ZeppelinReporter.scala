package de.frosner.ddq.reporters

import java.io.PrintStream

import de.frosner.ddq.constraints.{ConstraintError, ConstraintFailure, ConstraintSuccess}
import de.frosner.ddq.core.CheckResult

/**
 * A class which produces an %html report of [[CheckResult]].
 *
**/
case class ZeppelinReporter(stream: PrintStream = Console.out) extends PrintStreamReporter {

  private def inTd(s: String) = s"""<td style="padding:3px">$s</td>"""

  private var reportedSomething = false

  /**
    * Output zeppelin report of a given checkResult to the stream passed to the constructor.
    * If it is the first reported result, %html will be printed first.
    *
    * @param checkResult The [[CheckResult]] to be reported
   **/
  override def report(checkResult: CheckResult, header: String, prologue: String): Unit = {
    if (!reportedSomething) {
      stream.println("%html")
      reportedSomething = true
    }
    stream.println("</p>")
    stream.println(s"<h4>$header</h4>")
    stream.println(s"<h5>$prologue</h5>")
    if (checkResult.constraintResults.nonEmpty) {
      stream.println(s"<table>")
      checkResult.constraintResults.foreach {
        case (_, constraintResult) => {
          val resultString = constraintResult.status match {
            case ConstraintSuccess => "&#9989;"
            case ConstraintFailure => "&#10060;"
            case ConstraintError(throwable) => "&#9995;"
          }
          stream.println(s"<tr>${inTd(resultString)}${inTd(constraintResult.message)}</tr>")
        }
      }
      stream.println(s"</table>")
    } else {
      stream.println("Nothing to check!")
    }
    stream.println("<p hidden>")
  }

}
