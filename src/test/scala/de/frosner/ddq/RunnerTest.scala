package de.frosner.ddq

import java.io.{ByteArrayOutputStream, PrintStream}

import de.frosner.ddq.reporters.{MarkdownReporter, ConsoleReporter}
import org.apache.spark.sql.{DataFrame}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

class RunnerTest extends FlatSpec with Matchers with MockitoSugar {
  val df = mock[DataFrame]
  when(df.toString).thenReturn("[[column: int]]")
  when(df.count).thenReturn(3)
  when(df.columns).thenReturn(Array("column"))

  "runner" should "run with multiple checks" in {
    val constraintResults1 = List(ConstraintSuccess("The number of rows is equal to 3"))
    val constraintResults2 = List(ConstraintFailure("The actual number of rows 3 is not equal to the expected 4"))

    val check1 = Check(df, None, None, Seq(Constraint(df => constraintResults1.head)))
    val check2 = Check(df, None, None, Seq(Constraint(df => constraintResults2.head)))

    val checkResults = Runner.run(List(check1, check2), List())

    checkResults.size shouldBe 2

    val checkResult1 = checkResults.head
    val checkResult2 = checkResults.drop(1).head

    checkResult1.check shouldBe check1
    checkResult1.constraintResults shouldBe constraintResults1

    checkResult2.check shouldBe check2
    checkResult2.constraintResults shouldBe constraintResults2
  }

  it should "run with multiple reporters" in {
    val check = Check(df, None, None, Seq(Constraint(df => ConstraintSuccess("success"))))
    val baos1 = new ByteArrayOutputStream()
    val consoleReporter = new ConsoleReporter(new PrintStream(baos1))
    val baos2 = new ByteArrayOutputStream()
    val markdownReporter = new MarkdownReporter(new PrintStream(baos2))

    Runner.run(List(check), List(consoleReporter, markdownReporter))

    // tests that runner outputs something to both reporters
    baos1.toString.nonEmpty shouldBe true
    baos2.toString.nonEmpty shouldBe true
  }
}
