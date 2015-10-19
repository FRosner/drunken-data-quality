package de.frosner.ddq.check

import java.io.{ByteArrayOutputStream, PrintStream}

import de.frosner.ddq.reporters.{Reporter, ConsoleReporter, MarkdownReporter}
import org.apache.spark.sql.DataFrame
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

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
    val checkResult = check.run(List.empty)

    val reporter1 = mock[Reporter]
    val reporter2 = mock[Reporter]

    Runner.run(List(check), List(reporter1, reporter2))
    verify(reporter1).report(checkResult)
    verify(reporter2).report(checkResult)
  }
}
