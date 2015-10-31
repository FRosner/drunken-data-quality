package de.frosner.ddq.reporters

import java.io.{PrintStream, ByteArrayOutputStream}

import de.frosner.ddq.core._
import org.apache.spark.sql.DataFrame
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}

class MarkdownReporterTest extends FlatSpec with Matchers with MockitoSugar {

  "A Markdown reporter" should "produce correct output for a check with constraints" in {
    val baos = new ByteArrayOutputStream()
    val markdownReporter = new MarkdownReporter(new PrintStream(baos))

    val header = "Header"
    val prologue = "Prologue"
    val success = ConstraintSuccess("success")
    val failure = ConstraintFailure("failure")
    val constraints = Map(
      Constraint(df => success) -> success,
      Constraint(df => failure) -> failure
    )
    val check = Check(mock[DataFrame], Some("df"), Option.empty, constraints.keys.toSeq)

    markdownReporter.report(CheckResult(header, prologue, constraints, check))
    val expectedOutput = s"""**$header**

$prologue

- *SUCCESS*: ${success.message}
- *FAILURE*: ${failure.message}

"""

    baos.toString shouldBe expectedOutput
  }

  it should "produce correct output for a check without constraint" in {
    val baos = new ByteArrayOutputStream()
    val markdownReporter = new MarkdownReporter(new PrintStream(baos))

    val header = "Header"
    val prologue = "Prologue"
    val check = Check(mock[DataFrame], Some("df"), Option.empty, Seq.empty)

    markdownReporter.report(CheckResult(header, prologue, Map.empty, check))
    val expectedOutput = s"""**$header**

$prologue

Nothing to check!

"""

    baos.toString shouldBe expectedOutput
  }

}
