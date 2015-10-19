package de.frosner.ddq

import java.io.{PrintStream, ByteArrayOutputStream}

import de.frosner.ddq.reporters.{MarkdownReporter, ConsoleReporter}
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

class ReporterTest extends FlatSpec with Matchers with MockitoSugar {
  val df = mock[DataFrame]
  when(df.toString).thenReturn("[column: int]")
  when(df.count).thenReturn(3)
  when(df.columns).thenReturn(Array("column"))

  val check = Check(df, None, None, Seq(
    Constraint(df => ConstraintSuccess("The number of rows is equal to 3")),
    Constraint(df => ConstraintFailure("The actual number of rows 3 is not equal to the expected 2")),
    Constraint(df => ConstraintSuccess("Constraint column > 0 is satisfied")),
    Constraint(df => Hint("It's a mock"))
  ))

  "ConsoleReporter" should "produce correct output for check with constraints" in {
    val baos = new ByteArrayOutputStream()
    val consoleReporter = new ConsoleReporter(new PrintStream(baos))

    check.run(List(consoleReporter))

    val expectedOutput = s"""${Console.BLUE}Checking [column: int]${Console.RESET}
${Console.BLUE}It has a total number of 1 columns and 3 rows.${Console.RESET}
${Console.GREEN}- The number of rows is equal to 3${Console.RESET}
${Console.RED}- The actual number of rows 3 is not equal to the expected 2${Console.RESET}
${Console.GREEN}- Constraint column > 0 is satisfied${Console.RESET}
${Console.BLUE}It's a mock${Console.RESET}
"""
    baos.toString shouldBe expectedOutput
  }

  it should "produce correct output for check without constraints" in {
    val baos = new ByteArrayOutputStream()
    val consoleReporter = new ConsoleReporter(new PrintStream(baos))
    Check(df, None, None, Seq()).run(List(consoleReporter))

    val expectedOutput =
      s"""${Console.BLUE}Checking [column: int]${Console.RESET}
${Console.BLUE}It has a total number of 1 columns and 3 rows.${Console.RESET}
${Console.BLUE}Nothing to check${Console.RESET}
"""

    baos.toString shouldBe expectedOutput
  }

  "MarkdownReporter" should "produce correct output for check with constraints" in {
    val baos = new ByteArrayOutputStream()
    val markdownReporter = new MarkdownReporter(new PrintStream(baos))

    check.run(List(markdownReporter))

    val expectedOutput = """# Checking [column: int]

It has a total number of 1 columns and 3 rows.

* [success]: The number of rows is equal to 3
* [failure]: The actual number of rows 3 is not equal to the expected 2
* [success]: Constraint column > 0 is satisfied
* [hint]: It's a mock
"""

    baos.toString shouldBe expectedOutput
  }

  it should "produce correct output for check without constraint" in {
    val baos = new ByteArrayOutputStream()
    val markdownReporter = new MarkdownReporter(new PrintStream(baos))
    Check(df, None, None, Seq()).run(List(markdownReporter))

    val expectedOutput = s"""# Checking [column: int]

It has a total number of 1 columns and 3 rows.

Nothing to check
"""

    baos.toString shouldBe expectedOutput
  }
}

