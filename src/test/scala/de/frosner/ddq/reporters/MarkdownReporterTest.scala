package de.frosner.ddq.reporters

import java.io.{PrintStream, ByteArrayOutputStream}

import de.frosner.ddq.constraints._
import de.frosner.ddq.core._
import org.apache.spark.sql.DataFrame
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}

class MarkdownReporterTest extends FlatSpec with Matchers with MockitoSugar {

  "A Markdown reporter" should "produce correct output for a check with constraints" in {
    val baos = new ByteArrayOutputStream()
    val markdownReporter = new MarkdownReporter(new PrintStream(baos))

    val df = mock[DataFrame]
    val dfName = "myDf"
    val dfColumns = Array("1", "2")
    val dfCount = 5
    when(df.columns).thenReturn(dfColumns)

    val header = s"Checking $dfName"
    val prologue = s"It has a total number of ${dfColumns.size} columns and $dfCount rows."
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

    val check = Check(df, Some(dfName), Option.empty, constraints.keys.toSeq)

    markdownReporter.report(CheckResult(constraints, check, dfCount))
    val expectedOutput = s"""**$header**

$prologue

- *SUCCESS*: ${result1.message}
- *FAILURE*: ${result2.message}
- *ERROR*: ${result3.message}

"""

    baos.toString shouldBe expectedOutput
  }

  it should "produce correct output for a check without constraint" in {
    val baos = new ByteArrayOutputStream()
    val markdownReporter = new MarkdownReporter(new PrintStream(baos))

    val df = mock[DataFrame]
    val dfName = "myDf"
    val dfColumns = Array("1", "2")
    val dfCount = 5
    when(df.columns).thenReturn(dfColumns)

    val header = s"Checking $dfName"
    val prologue = s"It has a total number of ${dfColumns.size} columns and $dfCount rows."
    val check = Check(df, Some(dfName), Option.empty, Seq.empty)

    markdownReporter.report(CheckResult(Map.empty, check, dfCount))
    val expectedOutput = s"""**$header**

$prologue

Nothing to check!

"""

    baos.toString shouldBe expectedOutput
  }

}
