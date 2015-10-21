package de.frosner.ddq.check

import java.io.{ByteArrayOutputStream, PrintStream}

import de.frosner.ddq.reporters.{Reporter, ConsoleReporter, MarkdownReporter}
import org.apache.spark.sql.DataFrame
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

class RunnerTest extends FlatSpec with Matchers with MockitoSugar {

  "A runner" should "run with multiple checks" in {
    val df1 = mock[DataFrame]
    val df1ToString = "[column: IntegerType]"
    val df1Count = 3
    val df1Columns = Array("column")
    when(df1.toString).thenReturn(df1ToString)
    when(df1.count).thenReturn(df1Count)
    when(df1.columns).thenReturn(df1Columns)

    val df2 = mock[DataFrame]
    val df2ToString = "[id: StringType]"
    val df2Count = 5
    val df2Columns = Array("id")
    when(df2.toString).thenReturn(df2ToString)
    when(df2.count).thenReturn(df2Count)
    when(df2.columns).thenReturn(df2Columns)

    val result1 = ConstraintSuccess("The number of rows is equal to 3")
    val constraint1 = Constraint(df => result1)

    val result2 = ConstraintFailure("The actual number of rows 3 is not equal to the expected 4")
    val constraint2 = Constraint(df => result2)

    val check1 = Check(df1, None, None, Seq(constraint1))
    val check2 = Check(df2, None, None, Seq(constraint2))

    val checkResults = Runner.run(List(check1, check2), List.empty)

    checkResults.size shouldBe 2

    val checkResult1 = checkResults.head
    val checkResult2 = checkResults.drop(1).head

    checkResult1.check shouldBe check1
    checkResult1.constraintResults shouldBe Map((constraint1, result1))
    checkResult1.header shouldBe s"Checking $df1ToString"
    checkResult1.prologue shouldBe s"It has a total number of ${df1Columns.size} columns and $df1Count rows."

    checkResult2.check shouldBe check2
    checkResult2.constraintResults shouldBe Map((constraint2, result2))
    checkResult2.header shouldBe s"Checking $df2ToString"
    checkResult2.prologue shouldBe s"It has a total number of ${df2Columns.size} columns and $df2Count rows."
  }

  it should "show the display name in the header if it is set" in {
    val df = mock[DataFrame]
    when(df.toString).thenReturn("")
    when(df.count).thenReturn(1)
    when(df.columns).thenReturn(Array.empty[String])

    val displayName = "Amaze DataFrame"
    val check = Check(df, Some(displayName), None, Seq(Constraint(df => ConstraintSuccess("success"))))
    val checkResult = Runner.run(List(check), List.empty).head

    checkResult.header shouldBe s"Checking $displayName"
  }

  it should "report to all reporters what it returns" in {
    val df = mock[DataFrame]
    when(df.toString).thenReturn("")
    when(df.count).thenReturn(1)
    when(df.columns).thenReturn(Array.empty[String])

    val check = Check(df, None, None, Seq(Constraint(df => ConstraintSuccess("success"))))
    val checkResult = Runner.run(List(check), List.empty).head

    val reporter1 = mock[Reporter]
    val reporter2 = mock[Reporter]

    Runner.run(List(check), List(reporter1, reporter2))
    verify(reporter1).report(checkResult)
    verify(reporter2).report(checkResult)
  }

}
