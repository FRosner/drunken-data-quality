package de.frosner.ddq.reporters

import java.util.concurrent.TimeUnit

import courier.{Envelope, Mailer, Text}
import de.frosner.ddq.constraints._
import de.frosner.ddq.core.CheckResult
import courier._
import de.frosner.ddq.reporters

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * A class which produces an HTML report of [[CheckResult]] and sends it to the configured SMTP server.
  *
  * @param smtpServer URL of the SMTP server to use for sending the email
  * @param from Email address of the sender
  * @param to Email addresses of the receivers
  * @param cc Email addresses of the carbon copy receivers
  * @param smtpPort Port of the SMTP server to use for sending the email
  * @param reportOnlyOnFailure Whether to report only if there is a failing check (true) or always (false)
  * @param usernameAndPassword Optional credentials
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
                         usernameAndPassword: Option[(String, String)] = None,
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
  override def report(checkResult: CheckResult, header: String, prologue: String): Unit = synchronized {
    val numSuccessfulConstraints = computeSuccessful(checkResult)
    val numFailedConstraints = computeFailed(checkResult)
    val numErroredConstraints = computeErrored(checkResult)
    if (accumulatedReport) {
      reports = reports ++ Seq((checkResult, header, prologue))
    } else if (!(numFailedConstraints == 0 && numErroredConstraints == 0 && reportOnlyOnFailure)) {
      val checkName = checkResult.check.displayName.getOrElse(checkResult.check.dataFrame.toString())
      val subject = generateSubject(checkName, numFailedConstraints, numSuccessfulConstraints, numErroredConstraints)
      sendReport(
        subject = subject,
        message = generateReport(checkResult, header, prologue)
      )
    }
  }

  private def generateSubject(checkName: String, numFailed: Int, numSuccessful: Int, numErrored: Int): String = {
    val verb =
      if (numFailed > 0)
        if (numErrored > 0)
          "failed and errored"
        else
          "failed"
      else if (numErrored > 0)
        "errored"
      else if (numSuccessful > 0)
        "was successful"
      else
        "didn't do anything"
    s"Checking $checkName $verb" +
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
      if (!(numFailedConstraints == 0 && numErroredConstraints == 0 && reportOnlyOnFailure && !reportsToSend.isEmpty)) {
        val checkName = accumulatedCheckName.getOrElse(s"${checkResults.size} checks")
        val subject = generateSubject(checkName, numFailedConstraints, numSuccessfulConstraints, numErroredConstraints)
        val message = if (reportsToSend.isEmpty) {
          "No checks executed. Please run your checks before sending out a report."
        } else {
          "<p>\n" + reportsToSend.map {
            case (checkResult, header, prologue) => generateReport(checkResult, header, prologue)
          }.mkString("\n</p>\n<p>\n") + "\n</p>\n"
        }
        sendReport(subject, message)
      }
    } else {
      throw new IllegalStateException("Cannot trigger an accumulated report unless it is switched on (see constructor).")
    }
  }

  private def sendReport(subject: String, message: String): Unit = {
    val mailer = Mailer(smtpServer, smtpPort).startTtls(true)
    val maybeAuthenticatedMailer = usernameAndPassword.map {
      case (username, password) => mailer.as(username, password)
    }.getOrElse(mailer)
    val contentString = EmailReporter.htmlPrefix + message + EmailReporter.htmlSuffix
    val envelope = Envelope.from(from.addr)
      .to(to.map(_.addr).toSeq:_*)
      .cc(cc.map(_.addr).toSeq:_*)
      .subject(subjectPrefix + subject)
      .content(Multipart().html(contentString))
    val future = maybeAuthenticatedMailer()(envelope)
    Await.ready(future, Duration(5, TimeUnit.SECONDS))
  }

}

object EmailReporter {

  val htmlPrefix = "<html><head><meta http-equiv=\"Content-Type\" content=\"text/html charset=utf-8\"></head><body style=\"word-wrap: break-word; -webkit-nbsp-mode: space; -webkit-line-break: after-white-space;\" class=\"\">\n"
  val htmlSuffix = "</body></html>\n"

}