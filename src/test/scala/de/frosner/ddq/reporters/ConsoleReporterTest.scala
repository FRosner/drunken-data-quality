package de.frosner.ddq.reporters

import java.io.{ByteArrayOutputStream, PrintStream}

import de.frosner.ddq.core._
import org.apache.spark.sql.DataFrame
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

class ConsoleReporterTest extends FlatSpec with Matchers with MockitoSugar {

  "A Console reporter" should "produce correct output for a check with constraints" in {
    val baos = new ByteArrayOutputStream()
    val consoleReporter = new ConsoleReporter(new PrintStream(baos))

    val header = "Header"
    val prologue = "Prologue"
    val success = ConstraintSuccess("success")
    val failure = ConstraintFailure("failure")
    val constraints = Map(
      Constraint(df => success) -> success,
      Constraint(df => failure) -> failure
    )
    val check = Check(mock[DataFrame], Some("df"), Option.empty, constraints.keys.toSeq)

    consoleReporter.report(CheckResult(header, prologue, constraints, check))
    val expectedOutput = s"""${Console.BLUE}$header${Console.RESET}
${Console.BLUE}$prologue${Console.RESET}
${Console.GREEN}- ${success.message}${Console.RESET}
${Console.RED}- ${failure.message}${Console.RESET}
"""

    baos.toString shouldBe expectedOutput
  }

  it should "produce correct output for a check without constraint" in {
    val baos = new ByteArrayOutputStream()
    val consoleReporter = new ConsoleReporter(new PrintStream(baos))

    val header = "Header"
    val prologue = "Prologue"
    val check = Check(mock[DataFrame], Some("df"), Option.empty, Seq.empty)

    consoleReporter.report(CheckResult(header, prologue, Map.empty, check))
    val expectedOutput = s"""${Console.BLUE}$header${Console.RESET}
${Console.BLUE}$prologue${Console.RESET}
${Console.BLUE}Nothing to check!${Console.RESET}
"""

    baos.toString shouldBe expectedOutput
  }

}
