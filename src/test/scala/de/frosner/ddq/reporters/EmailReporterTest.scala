package de.frosner.ddq.reporters

import de.frosner.ddq.constraints._
import de.frosner.ddq.core._
import de.frosner.ddq.testutils.{DummyConstraint, DummyConstraintResult}
import org.apache.spark.sql.DataFrame
import org.jvnet.mock_javamail.Mailbox
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

class EmailReporterTest extends FlatSpec with Matchers with MockitoSugar with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    Mailbox.clearAll()
  }

  "An email reporter" should "produce correct output for a check with failing, erroring and successful constraints" in {
    val toReceiver = "test@receivers.de"
    val subjectPrefix = "[DDQ]"

    val reporter = EmailReporter(
      smtpServer = "test@smtp.de",
      to = Set(toReceiver),
      cc = Set.empty,
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

    val inbox = Mailbox.get(toReceiver)
    inbox.size shouldBe 1
    val msg = inbox.get(0)
    msg.getSubject shouldBe subjectPrefix + s"Checking $displayName failed and errored (1 successful, 1 failed, 1 errored)"
    msg.getContent shouldBe s"""<h4>$header</h4>
<h5>$prologue</h5>
<table>
<tr><td style="padding:3px">&#9989;</td><td style="padding:3px">$message1</td></tr>
<tr><td style="padding:3px">&#10060;</td><td style="padding:3px">$message2</td></tr>
<tr><td style="padding:3px">&#9995;</td><td style="padding:3px">$message3</td></tr>
</table>"""
  }

  it should "produce correct output for a check with failed and successful constraints" in {
    val toReceiver = "test@receivers.de"
    val subjectPrefix = "[DDQ]"

    val reporter = EmailReporter(
      smtpServer = "test@smtp.de",
      to = Set(toReceiver),
      cc = Set.empty,
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

    val constraints = Map[Constraint, ConstraintResult[Constraint]](
      constraint1 -> result1,
      constraint2 -> result2
    )
    val check = Check(df, Some(displayName), Option.empty, constraints.keys.toSeq)

    reporter.report(CheckResult(constraints, check, dfCount))

    val inbox = Mailbox.get(toReceiver)
    inbox.size shouldBe 1
    val msg = inbox.get(0)
    msg.getSubject shouldBe subjectPrefix + s"Checking $displayName failed (1 successful, 1 failed, 0 errored)"
    msg.getContent shouldBe s"""<h4>$header</h4>
<h5>$prologue</h5>
<table>
<tr><td style="padding:3px">&#9989;</td><td style="padding:3px">$message1</td></tr>
<tr><td style="padding:3px">&#10060;</td><td style="padding:3px">$message2</td></tr>
</table>"""
  }

  it should "produce correct output for a check with errored and successful constraints" in {
    val toReceiver = "test@receivers.de"
    val subjectPrefix = "[DDQ]"

    val reporter = EmailReporter(
      smtpServer = "test@smtp.de",
      to = Set(toReceiver),
      cc = Set.empty,
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
    val status2 = ConstraintError(new IllegalArgumentException())
    val constraint2 = DummyConstraint(message2, status2)
    val result2 = constraint2.fun(df)

    val constraints = Map[Constraint, ConstraintResult[Constraint]](
      constraint1 -> result1,
      constraint2 -> result2
    )
    val check = Check(df, Some(displayName), Option.empty, constraints.keys.toSeq)

    reporter.report(CheckResult(constraints, check, dfCount))

    val inbox = Mailbox.get(toReceiver)
    inbox.size shouldBe 1
    val msg = inbox.get(0)
    msg.getSubject shouldBe subjectPrefix + s"Checking $displayName errored (1 successful, 0 failed, 1 errored)"
    msg.getContent shouldBe s"""<h4>$header</h4>
<h5>$prologue</h5>
<table>
<tr><td style="padding:3px">&#9989;</td><td style="padding:3px">$message1</td></tr>
<tr><td style="padding:3px">&#9995;</td><td style="padding:3px">$message2</td></tr>
</table>"""
  }

  it should "produce correct output for a check with only successful constraints" in {
    val toReceiver = "test@receivers.de"
    val subjectPrefix = "[DDQ]"

    val reporter = EmailReporter(
      smtpServer = "test@smtp.de",
      to = Set(toReceiver),
      cc = Set.empty,
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
    val status2 = ConstraintSuccess
    val constraint2 = DummyConstraint(message2, status2)
    val result2 = constraint2.fun(df)

    val constraints = Map[Constraint, ConstraintResult[Constraint]](
      constraint1 -> result1,
      constraint2 -> result2
    )
    val check = Check(df, Some(displayName), Option.empty, constraints.keys.toSeq)

    reporter.report(CheckResult(constraints, check, dfCount))

    val inbox = Mailbox.get(toReceiver)
    inbox.size shouldBe 1
    val msg = inbox.get(0)
    msg.getSubject shouldBe subjectPrefix + s"Checking $displayName was successful (2 successful, 0 failed, 0 errored)"
    msg.getContent shouldBe s"""<h4>$header</h4>
<h5>$prologue</h5>
<table>
<tr><td style="padding:3px">&#9989;</td><td style="padding:3px">$message1</td></tr>
<tr><td style="padding:3px">&#9989;</td><td style="padding:3px">$message2</td></tr>
</table>"""
  }

  it should "produce correct output for a check without constraint" in {
    val toReceiver = "test@receivers.de"
    val subjectPrefix = "[DDQ]"

    val reporter = EmailReporter(
      smtpServer = "test@smtp.de",
      to = Set(toReceiver),
      cc = Set.empty,
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

    val constraints = Map.empty[Constraint, ConstraintResult[Constraint]]
    val check = Check(df, Some(displayName), Option.empty, constraints.keys.toSeq)

    reporter.report(CheckResult(constraints, check, dfCount))

    val inbox = Mailbox.get(toReceiver)
    inbox.size shouldBe 1
    val msg = inbox.get(0)
    msg.getSubject shouldBe subjectPrefix + s"Checking $displayName didn't do anything (0 successful, 0 failed, 0 errored)"
    msg.getContent shouldBe s"""<h4>$header</h4>
<h5>$prologue</h5>
Nothing to check!"""
  }

  it should "send the email to everyone (including the cced people)" in {
    val toReceiver = "test@receivers.de"
    val ccReceiver1 = "cc1@receivers.de"
    val ccReceiver2 = "cc2@receivers.de"
    val subjectPrefix = "[DDQ]"

    val reporter = EmailReporter(
      smtpServer = "test@smtp.de",
      to = Set(toReceiver),
      cc = Set(ccReceiver1, ccReceiver2),
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

    val constraints = Map.empty[Constraint, ConstraintResult[Constraint]]
    val check = Check(df, Some(displayName), Option.empty, constraints.keys.toSeq)

    reporter.report(CheckResult(constraints, check, dfCount))

    Mailbox.get(toReceiver).size shouldBe 1
    Mailbox.get(ccReceiver1).size shouldBe 1
    Mailbox.get(ccReceiver2).size shouldBe 1
  }

  it should "not report if only on failure is enabled and there are no failures" in {
    val toReceiver = "test@receivers.de"
    val subjectPrefix = "[DDQ]"

    val reporter = EmailReporter(
      smtpServer = "test@smtp.de",
      to = Set(toReceiver),
      cc = Set.empty,
      subjectPrefix = subjectPrefix,
      smtpPort = 25,
      from = "test@senders.de",
      username = None,
      password = None,
      reportOnlyOnFailure = true,
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
    val status2 = ConstraintSuccess
    val constraint2 = DummyConstraint(message2, status2)
    val result2 = constraint2.fun(df)

    val constraints = Map[Constraint, ConstraintResult[Constraint]](
      constraint1 -> result1,
      constraint2 -> result2
    )
    val check = Check(df, Some(displayName), Option.empty, constraints.keys.toSeq)
    reporter.report(CheckResult(constraints, check, dfCount))

    Mailbox.get(toReceiver).size shouldBe 0
  }

  it should "report if only on failure is enabled and there are failures" in {
    val toReceiver = "test@receivers.de"
    val subjectPrefix = "[DDQ]"

    val reporter = EmailReporter(
      smtpServer = "test@smtp.de",
      to = Set(toReceiver),
      cc = Set.empty,
      subjectPrefix = subjectPrefix,
      smtpPort = 25,
      from = "test@senders.de",
      username = None,
      password = None,
      reportOnlyOnFailure = true,
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

    val constraints = Map[Constraint, ConstraintResult[Constraint]](
      constraint1 -> result1,
      constraint2 -> result2
    )
    val check = Check(df, Some(displayName), Option.empty, constraints.keys.toSeq)
    reporter.report(CheckResult(constraints, check, dfCount))

    Mailbox.get(toReceiver).size shouldBe 1
  }

  "An accumulating email reporter" should "produce correct output for a check with constraints" in {
    val toReceiver = "test@receivers.de"
    val subjectPrefix = "[DDQ]"

    val reporter = EmailReporter(
      smtpServer = "test@smtp.de",
      to = Set(toReceiver),
      cc = Set.empty,
      subjectPrefix = subjectPrefix,
      smtpPort = 25,
      from = "test@senders.de",
      username = None,
      password = None,
      reportOnlyOnFailure = false,
      accumulatedReport = true
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
    reporter.report(CheckResult(constraints, check, dfCount))

    val inbox = Mailbox.get(toReceiver)
    inbox.size shouldBe 0

    reporter.sendAccumulatedReport(None)
    inbox.size shouldBe 1

    val expectedCheck = s"""<h4>$header</h4>
<h5>$prologue</h5>
<table>
<tr><td style="padding:3px">&#9989;</td><td style="padding:3px">$message1</td></tr>
<tr><td style="padding:3px">&#10060;</td><td style="padding:3px">$message2</td></tr>
<tr><td style="padding:3px">&#9995;</td><td style="padding:3px">$message3</td></tr>
</table>"""

    val msg = inbox.get(0)
    msg.getSubject shouldBe subjectPrefix + s"Checking 2 checks failed and errored (2 successful, 2 failed, 2 errored)"
    msg.getContent shouldBe s"""<p>
$expectedCheck
</p>
<p>
$expectedCheck
</p>
"""
  }

  it should "produce correct output for a check without constraint" in {
    val toReceiver = "test@receivers.de"
    val subjectPrefix = "[DDQ]"

    val reporter = EmailReporter(
      smtpServer = "test@smtp.de",
      to = Set(toReceiver),
      cc = Set.empty,
      subjectPrefix = subjectPrefix,
      smtpPort = 25,
      from = "test@senders.de",
      username = None,
      password = None,
      reportOnlyOnFailure = false,
      accumulatedReport = true
    )

    val df = mock[DataFrame]
    val displayName = "myDf"
    val dfColumns = Array("1", "2")
    val dfCount = 5
    when(df.columns).thenReturn(dfColumns)

    val header = s"Checking $displayName"
    val prologue = s"It has a total number of ${dfColumns.length} columns and $dfCount rows."

    val constraints = Map.empty[Constraint, ConstraintResult[Constraint]]
    val check = Check(df, Some(displayName), Option.empty, constraints.keys.toSeq)

    reporter.report(CheckResult(constraints, check, dfCount))
    reporter.sendAccumulatedReport(Some(displayName))

    val inbox = Mailbox.get(toReceiver)
    inbox.size shouldBe 1
    val msg = inbox.get(0)
    msg.getSubject shouldBe subjectPrefix + s"Checking $displayName didn't do anything (0 successful, 0 failed, 0 errored)"
    msg.getContent shouldBe s"""<p>
<h4>$header</h4>
<h5>$prologue</h5>
Nothing to check!
</p>
"""
  }

  it should "send an informative email if triggered without any previous reports" in {
    val toReceiver = "test@receivers.de"
    val subjectPrefix = "[DDQ]"

    val reporter = EmailReporter(
      smtpServer = "test@smtp.de",
      to = Set(toReceiver),
      cc = Set.empty,
      subjectPrefix = subjectPrefix,
      smtpPort = 25,
      from = "test@senders.de",
      username = None,
      password = None,
      reportOnlyOnFailure = false,
      accumulatedReport = true
    )

    val df = mock[DataFrame]
    val displayName = "myDf"
    val dfColumns = Array("1", "2")
    val dfCount = 5
    when(df.columns).thenReturn(dfColumns)

    val header = s"Checking $displayName"
    val prologue = s"It has a total number of ${dfColumns.length} columns and $dfCount rows."

    val constraints = Map.empty[Constraint, ConstraintResult[Constraint]]
    val check = Check(df, Some(displayName), Option.empty, constraints.keys.toSeq)

    reporter.sendAccumulatedReport(Some(displayName))

    val inbox = Mailbox.get(toReceiver)
    inbox.size shouldBe 1
    val msg = inbox.get(0)
    msg.getSubject shouldBe subjectPrefix + s"Checking $displayName didn't do anything (0 successful, 0 failed, 0 errored)"
    msg.getContent shouldBe "No checks executed. Please run your checks before sending out a report."
  }

  it should "not report if only on failure reporting is enabled and there are no failures" in {
    val toReceiver = "test@receivers.de"
    val subjectPrefix = "[DDQ]"

    val reporter = EmailReporter(
      smtpServer = "test@smtp.de",
      to = Set(toReceiver),
      cc = Set.empty,
      subjectPrefix = subjectPrefix,
      smtpPort = 25,
      from = "test@senders.de",
      username = None,
      password = None,
      reportOnlyOnFailure = true,
      accumulatedReport = true
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
    val status2 = ConstraintSuccess
    val constraint2 = DummyConstraint(message2, status2)
    val result2 = constraint2.fun(df)

    val constraints = Map[Constraint, ConstraintResult[Constraint]](
      constraint1 -> result1,
      constraint2 -> result2
    )
    val check = Check(df, Some(displayName), Option.empty, constraints.keys.toSeq)

    reporter.report(CheckResult(constraints, check, dfCount))
    reporter.report(CheckResult(constraints, check, dfCount))

    reporter.sendAccumulatedReport(None)
    Mailbox.get(toReceiver).size shouldBe 0
  }

  it should "report if only on failure reporting is enabled and there are failures" in {
    val toReceiver = "test@receivers.de"
    val subjectPrefix = "[DDQ]"

    val reporter = EmailReporter(
      smtpServer = "test@smtp.de",
      to = Set(toReceiver),
      cc = Set.empty,
      subjectPrefix = subjectPrefix,
      smtpPort = 25,
      from = "test@senders.de",
      username = None,
      password = None,
      reportOnlyOnFailure = true,
      accumulatedReport = true
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

    val constraints = Map[Constraint, ConstraintResult[Constraint]](
      constraint1 -> result1,
      constraint2 -> result2
    )
    val check = Check(df, Some(displayName), Option.empty, constraints.keys.toSeq)

    reporter.report(CheckResult(constraints, check, dfCount))
    reporter.report(CheckResult(constraints, check, dfCount))

    reporter.sendAccumulatedReport(None)
    Mailbox.get(toReceiver).size shouldBe 1
  }

  it should "not report the same accumulated report twice" in {
    val toReceiver = "test@receivers.de"
    val subjectPrefix = "[DDQ]"

    val reporter = EmailReporter(
      smtpServer = "test@smtp.de",
      to = Set(toReceiver),
      cc = Set.empty,
      subjectPrefix = subjectPrefix,
      smtpPort = 25,
      from = "test@senders.de",
      username = None,
      password = None,
      reportOnlyOnFailure = true,
      accumulatedReport = true
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

    val constraints = Map[Constraint, ConstraintResult[Constraint]](
      constraint1 -> result1,
      constraint2 -> result2
    )
    val check = Check(df, Some(displayName), Option.empty, constraints.keys.toSeq)

    reporter.report(CheckResult(constraints, check, dfCount))
    reporter.report(CheckResult(constraints, check, dfCount))

    reporter.sendAccumulatedReport(Some(displayName))
    reporter.sendAccumulatedReport(Some(displayName))
    val inbox = Mailbox.get(toReceiver)
    inbox.size shouldBe 2
    val msg = inbox.get(1)
    msg.getSubject shouldBe subjectPrefix + s"Checking $displayName didn't do anything (0 successful, 0 failed, 0 errored)"
    msg.getContent shouldBe "No checks executed. Please run your checks before sending out a report."
  }

  it should "throw an exception if triggered but switched off" in {
    intercept[IllegalStateException] {
      val toReceiver = "test@receivers.de"
      val subjectPrefix = "[DDQ]"

      val reporter = EmailReporter(
        smtpServer = "test@smtp.de",
        to = Set(toReceiver),
        cc = Set.empty,
        subjectPrefix = subjectPrefix,
        smtpPort = 25,
        from = "test@senders.de",
        username = None,
        password = None,
        reportOnlyOnFailure = true,
        accumulatedReport = false
      )

      reporter.sendAccumulatedReport(None)
    }
  }

}
