package de.frosner.ddq.reporters

import java.io.{ByteArrayOutputStream, PrintStream}
import java.util.concurrent.TimeUnit
import javax.mail.internet.InternetAddress

import courier._
import de.frosner.ddq.constraints._
import de.frosner.ddq.core._
import de.frosner.ddq.testutils.{DummyConstraint, DummyConstraintResult}
import org.apache.spark.sql.DataFrame
import org.jvnet.mock_javamail.Mailbox
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration.Duration


class EmailReporterTest extends FlatSpec with Matchers with MockitoSugar {

  "An email reporter" should "produce correct output for a check with constraints" in {
    val toReceiver = "test@receivers.de"
    val subjectPrefix = "[DDQ]"

    val reporter = EmailReporter(
      smtpServer = "test@smtp.de",
      to = Set(toReceiver),
      cc = Set("cc1@receivers.de", "cc2@receivers.de"),
      subjectPrefix = subjectPrefix,
      smtpPort = 25,
      from = "test@senders.de",
      username = None,
      password = None,
      reportOnlyOnFailure = false,
      accumulatedReport = false
    )

    val df = mock[DataFrame]
    val displayName = "myDf"
    val dfColumns = Array("1", "2")
    val dfCount = 5
    when(df.columns).thenReturn(dfColumns)

    val header = s"Checking $displayName"
    val prologue = s"It has a total number of ${dfColumns.length} columns and $dfCount rows."

    val message1 = "1"
    val status1 = ConstraintSuccess
    val constraint1 = DummyConstraint(message1, status1)
    val result1 = constraint1.fun(df)

    val message2 = "2"
    val status2 = ConstraintFailure
    val constraint2 = DummyConstraint(message2, status2)
    val result2 = constraint2.fun(df)

    val message3 = "3"
    val status3 = ConstraintError(new IllegalArgumentException())
    val constraint3 = DummyConstraint(message3, status3)
    val result3 = DummyConstraintResult(constraint3, message3, status3)

    val constraints = Map[Constraint, ConstraintResult[Constraint]](
      constraint1 -> result1,
      constraint2 -> result2,
      constraint3 -> result3
    )
    val check = Check(df, Some(displayName), Option.empty, constraints.keys.toSeq)

    reporter.report(CheckResult(constraints, check, dfCount))

    Thread.sleep(5000)
    val momsInbox = Mailbox.get(toReceiver)
    momsInbox.size shouldBe 1
    val momsMsg = momsInbox.get(0)
    momsMsg.getSubject shouldBe subjectPrefix + s"Checking $displayName failed (1 successful, 1 failed, 1 errored)"
    momsMsg.getContent shouldBe s"""<h4>$header</h4>
<h5>$prologue</h5>
<table>
<tr><td style="padding:3px">&#9989;</td><td style="padding:3px">$message1</td></tr>
<tr><td style="padding:3px">&#10060;</td><td style="padding:3px">$message2</td></tr>
<tr><td style="padding:3px">&#9995;</td><td style="padding:3px">$message3</td></tr>
</table>"""
  }

  it should "produce correct output for a check without constraint" in {
    val baos = new ByteArrayOutputStream()
    val zeppelinReporter = ZeppelinReporter(new PrintStream(baos))

    val df = mock[DataFrame]
    val displayName = "myDf"
    val dfColumns = Array("1", "2")
    val dfCount = 5
    when(df.columns).thenReturn(dfColumns)

    val header = s"Checking $displayName"
    val prologue = s"It has a total number of ${dfColumns.length} columns and $dfCount rows."

    val constraints = Map.empty[Constraint, ConstraintResult[Constraint]]
    val check = Check(df, Some(displayName), Option.empty, constraints.keys.toSeq)

    zeppelinReporter.report(CheckResult(constraints, check, dfCount))
    val expectedOutput = s"""%html
</p>
<h4>$header</h4>
<h5>$prologue</h5>
Nothing to check!
<p hidden>
"""

    baos.toString shouldBe expectedOutput
  }

  it should "send the email to everyone (including the cced people)" in {

  }

  it should "only report failures if configured to" in {

  }

  "An accumulating email reporter" should "produce correct output for a check with constraints" in {
  }

  it should "produce correct output for a check without constraint" in {
  }

  it should "send the email to everyone (including the cced people)" in {
  }

  it should "only report failures if configured to" in {
  }

}
