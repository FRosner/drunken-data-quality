package de.frosner.ddq

import java.io.{ByteArrayOutputStream, PrintStream}

import de.frosner.ddq.reporters.{MarkdownReporter, ConsoleReporter}
import org.scalatest.Matchers

class RunnerTest extends TestDataFrameContext with Matchers {

  "runner" should "run with multiple checks" in {
    val check1 = Check(makeIntegerDf(List(1, 2, 3))).hasNumRowsEqualTo(3)
    val check2 = Check(makeIntegerDf(List(1, 2, 3))).hasNumRowsEqualTo(4)
    val checkResults = Runner.run(List(check1, check2), List())

    checkResults.size shouldBe 2

    val constraintResults1 = List(ConstraintSuccess("The number of rows is equal to 3"))
    val constraintResults2 = List(ConstraintFailure("The actual number of rows 3 is not equal to the expected 4"))

    val checkResult1 = checkResults.head
    val checkResult2 = checkResults.drop(1).head

    checkResult1.check shouldBe check1
    checkResult1.constraintResults shouldBe constraintResults1

    checkResult2.check shouldBe check2
    checkResult2.constraintResults shouldBe constraintResults2
  }

  it should "run with multiple reporters" in {
    val check = Check(makeIntegerDf(List(1, 2, 3))).hasNumRowsEqualTo(3)
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
