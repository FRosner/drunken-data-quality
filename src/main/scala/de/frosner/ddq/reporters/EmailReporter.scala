package de.frosner.ddq.reporters

import courier.{Envelope, Mailer, Text}
import de.frosner.ddq.constraints._
import de.frosner.ddq.core.CheckResult
import courier._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * A class which produces an HTML report of [[CheckResult]] and sends it to the configured SMTP server.
  *
  * @param smtpServer URL of the SMTP server to use for sending the email
  * @param from Email address of the sender
  * @param to Email addresses of the receivers
  * @param to Email addresses of the carbon copy receivers
  * @param smtpPort Port of the SMTP server to use for sending the email
  * @param reportOnlyOnFailure Whether to report only if there is a failing check (true) or always (false)
  * @param username Whether to and if so which username to use
  * @param password Whether to and if so which password to use
  * @param accumulatedReport Whether to report for each check result (false) or only when a report is triggered (true).
  *                          The accumulated option requires the reporter to stick around until manually triggered
  *                          or else you will lose the results.
  *
 **/
@volatile
case class EmailReporter(smtpServer: String,
                         to: Set[String],
                         cc: Set[String] = Set.empty,
                         subjectPrefix: String = "Data Quality Report: ",
                         smtpPort: Int = 25,
                         from: String = "mail@ddq.io",
                         username: Option[String] = None,
                         password: Option[String] = None,
                         reportOnlyOnFailure: Boolean = false,
                         accumulatedReport: Boolean = false
                        ) extends HumanReadableReporter {

  private def inTd(s: String) = s"""<td style="padding:3px">$s</td>"""

  type Header = String
  type Prologue = String

  @volatile
  private var reports: Seq[(CheckResult, Header, Prologue)] = Seq.empty

  /**
    * Send an email for the corresponding check result (unless accumulated reports are enabled).
    *
    * @param checkResult The [[CheckResult]] to be reported
   **/
  override def report(checkResult: CheckResult, header: String, prologue: String): Unit = {
    if (accumulatedReport) {
      reports = reports ++ Seq((checkResult, header, prologue))
    } else {
      val checkName = checkResult.check.displayName.getOrElse(checkResult.check.dataFrame.toString())
      val numFailedConstraints = computeFailed(checkResult)
      val numSuccessfulConstraints = computeSuccessful(checkResult)
      val numErroredConstraints = computeErrored(checkResult)
      val subject = generateSubject(checkName, numFailedConstraints, numSuccessfulConstraints, numErroredConstraints)
      sendReport(
        subject = subject,
        message = generateReport(checkResult, header, prologue)
      )
    }
  }

  private def generateSubject(checkName: String, numFailed: Int, numSuccessful: Int, numErrored: Int): String = {
    s"Checking $checkName ${if (numFailed > 0) "failed" else "successful"}" +
      s" ($numSuccessful successful, $numFailed failed, $numErrored errored)"
  }

  private def computeFailed(checkResult: CheckResult): Int =
    computeMetrics(checkResult, (result: ConstraintResult[Constraint]) => if (result.status == ConstraintFailure) 1 else 0)

  private def computeSuccessful(checkResult: CheckResult): Int =
    computeMetrics(checkResult, (result: ConstraintResult[Constraint]) => if (result.status == ConstraintSuccess) 1 else 0)

  private def computeErrored(checkResult: CheckResult): Int =
    computeMetrics(checkResult, (result: ConstraintResult[Constraint]) => if (result.status.isInstanceOf[ConstraintError]) 1 else 0)

  private def computeMetrics(checkResult: CheckResult, computer: ConstraintResult[Constraint] => Int): Int =
    checkResult.constraintResults.map {
      case (constraint, result) => computer(result)
    }.sum

  private def generateReport(checkResult: CheckResult, header: String, prologue: String): String = {
    val headerHtml = s"<h4>$header</h4>\n"
    val prologueHtml = s"<h5>$prologue</h5>\n"
    val bodyHtml = if (checkResult.constraintResults.nonEmpty) {
      val tableContent = checkResult.constraintResults.map {
        case (_, constraintResult) =>
          val resultString = constraintResult.status match {
            case ConstraintSuccess => "&#9989;"
            case ConstraintFailure => "&#10060;"
            case ConstraintError(throwable) => "&#9995;"
          }
          s"<tr>${inTd(resultString)}${inTd(constraintResult.message)}</tr>"
      }
      "<table>\n" + tableContent.mkString("\n") + "\n</table>"
    } else {
      "Nothing to check!"
    }
    headerHtml + prologueHtml + bodyHtml
  }

  def sendAccumulatedReport(accumulatedCheckName: Option[String]) = synchronized {
    if (accumulatedReport) {
      val reportsToSend = reports
      reports = Seq.empty
      val checkResults = reportsToSend.map {
        case (checkResult, header, prologue) => checkResult
      }
      val numFailedConstraints = checkResults.map(computeFailed).sum
      val numSuccessfulConstraints = checkResults.map(computeSuccessful).sum
      val numErroredConstraints = checkResults.map(computeErrored).sum
      val checkName = accumulatedCheckName.getOrElse(s"${checkResults.size} checks")
      val subject = generateSubject(checkName, numFailedConstraints, numSuccessfulConstraints, numErroredConstraints)
      sendReport(
        subject = subject,
        message = "<p>\n" + reports.map {
          case (checkResult, header, prologue) => generateReport(checkResult, header, prologue)
        }.mkString("</p><p>\n") + "</p>\n"
      )
    } else {
      throw new IllegalStateException("Cannot trigger an accumulated report unless it is switched on (see constructor).")
    }
  }

  private def sendReport(subject: String, message: String): Unit = {
    val mailer = Mailer(smtpServer, smtpPort)()
    mailer(Envelope.from(from.addr)
      .to(to.map(_.addr).toSeq:_*)
      .cc(cc.map(_.addr).toSeq:_*)
      .subject(subjectPrefix + subject)
      .content(Text(message)))
  }

}
