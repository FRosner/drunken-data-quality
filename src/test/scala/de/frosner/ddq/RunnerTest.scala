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
    val result1 = ConstraintSuccess("The number of rows is equal to 3")
    val constraint1 = Constraint(df => result1)

    val result2 = ConstraintFailure("The actual number of rows 3 is not equal to the expected 4")
    val constraint2 = Constraint(df => result2)

    val check1 = Check(df, None, None, Seq(constraint1))
    val check2 = Check(df, None, None, Seq(constraint2))

    val checkResults = Runner.run(List(check1, check2), List())

    checkResults.size shouldBe 2

    val checkResult1 = checkResults.head
    val checkResult2 = checkResults.drop(1).head

    checkResult1.check shouldBe check1
    checkResult1.constraintResults shouldBe Map((constraint1, result1))

    checkResult2.check shouldBe check2
    checkResult2.constraintResults shouldBe Map((constraint2, result2))
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
