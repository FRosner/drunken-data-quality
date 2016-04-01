package de.frosner.ddq.reporters

import java.io.{ByteArrayOutputStream, PrintStream}

import de.frosner.ddq.constraints._
import de.frosner.ddq.core._
import de.frosner.ddq.testutils.{DummyConstraint, DummyConstraintResult}
import org.apache.spark.sql.DataFrame
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

class ZeppelinReporterTest extends FlatSpec with Matchers with MockitoSugar {

  "A Console reporter" should "produce correct output for a check with constraints" in {
    val baos = new ByteArrayOutputStream()
    val zeppelinReporter = ZeppelinReporter(new PrintStream(baos))

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

    zeppelinReporter.report(CheckResult(constraints, check, dfCount))
    val expectedOutput = s"""%html
</p>
<h4>$header</h4>
<h5>$prologue</h5>
<table>
<tr><td style="padding:3px">&#9989;</td><td style="padding:3px">$message1</td></tr>
<tr><td style="padding:3px">&#10060;</td><td style="padding:3px">$message2</td></tr>
<tr><td style="padding:3px">&#9995;</td><td style="padding:3px">$message3</td></tr>
</table>
<p hidden>
"""

    baos.toString shouldBe expectedOutput
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

  it should "only print %html once for the first check" in {
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
    zeppelinReporter.report(CheckResult(constraints, check, dfCount))
    val expectedBody = s"""</p>
<h4>$header</h4>
<h5>$prologue</h5>
Nothing to check!
<p hidden>"""
    val expectedOutput = s"""%html
$expectedBody
$expectedBody
"""

    baos.toString shouldBe expectedOutput
  }

}
