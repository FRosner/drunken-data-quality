package de.frosner.ddq.reporters

import java.io.{ByteArrayOutputStream, PrintStream}

import de.frosner.ddq.constraints._
import de.frosner.ddq.core._
import org.apache.spark.sql.DataFrame
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

class ConsoleReporterTest extends FlatSpec with Matchers with MockitoSugar {

  "A Console reporter" should "produce correct output for a check with constraints" in {
    val baos = new ByteArrayOutputStream()
    val consoleReporter = new ConsoleReporter(new PrintStream(baos))

    val df = mock[DataFrame]
    val displayName = "myDf"
    val dfColumns = Array("1", "2")
    val dfCount = 5
    when(df.columns).thenReturn(dfColumns)

    val header = s"Checking $displayName"
    val prologue = s"It has a total number of ${dfColumns.size} columns and $dfCount rows."

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

    consoleReporter.report(CheckResult(constraints, check, dfCount))
    val expectedOutput = s"""${Console.BLUE}$header${Console.RESET}
${Console.BLUE}$prologue${Console.RESET}
${Console.GREEN}- ${result1.message}${Console.RESET}
${Console.RED}- ${result2.message}${Console.RESET}

"""

    baos.toString shouldBe expectedOutput
  }

  it should "produce correct output for a check without constraint" in {
    val baos = new ByteArrayOutputStream()
    val consoleReporter = new ConsoleReporter(new PrintStream(baos))

    val df = mock[DataFrame]
    val displayName = "myDf"
    val dfColumns = Array("1", "2")
    val dfCount = 5
    when(df.columns).thenReturn(dfColumns)

    val header = s"Checking $displayName"
    val prologue = s"It has a total number of ${dfColumns.size} columns and $dfCount rows."
    val check = Check(df, Some(displayName), Option.empty, Seq.empty)

    consoleReporter.report(CheckResult(Map.empty, check, dfCount))
    val expectedOutput = s"""${Console.BLUE}$header${Console.RESET}
${Console.BLUE}$prologue${Console.RESET}
${Console.BLUE}Nothing to check!${Console.RESET}

"""

    baos.toString shouldBe expectedOutput
  }

}
