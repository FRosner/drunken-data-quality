package de.frosner.ddq.core

import java.io.{ByteArrayOutputStream, FileDescriptor, FileOutputStream, PrintStream}

import de.frosner.ddq.constraints._
import de.frosner.ddq.reporters.{ConsoleReporter, Reporter}
import de.frosner.ddq.testutils.{SparkContexts, TestData}
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}

class CheckTest extends FlatSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with MockitoSugar with SparkContexts {

  override def afterAll(): Unit = resetSpark()

  "Multiple checks" should "produce a constraintResults map with all constraints and corresponding results" in {
    val expectedNumberOfRows1 = 3
    val expectedNumberOfRows2 = 2
    val constraintString = "column > 0"
    val columnName = "column"
    val check = Check(TestData.makeIntegerDf(spark, List(1, 2, 3)))
      .isAlwaysNull(columnName)
      .isNeverNull(columnName)
      .satisfies(constraintString)
    val constraint1 = check.constraints(0)
    val constraint2 = check.constraints(1)
    val constraint3 = check.constraints(2)

    check.run().constraintResults shouldBe Map(
      constraint1 -> AlwaysNullConstraintResult(
        constraint = AlwaysNullConstraint(columnName),
        data = Some(AlwaysNullConstraintResultData(3L)),
        status = ConstraintFailure
      ),
      constraint2 -> NeverNullConstraintResult(
        constraint = NeverNullConstraint(columnName),
        data = Some(NeverNullConstraintResultData(0L)),
        status = ConstraintSuccess
      ),
      constraint3 -> StringColumnConstraintResult(
        constraint = StringColumnConstraint(constraintString),
        data = Some(StringColumnConstraintResultData(0L)),
        status = ConstraintSuccess
      )
    )
  }

  "A check from a SQLContext" should "load the given table" in {
    val df = TestData.makeIntegerDf(spark, List(1, 2, 3))
    val tableName = "myintegerdf1"
    df.createOrReplaceTempView(tableName)
    val columnName = "column"
    val constraint = Check.isNeverNull(columnName)
    val result = NeverNullConstraintResult(
      constraint = NeverNullConstraint(columnName),
      data = Some(NeverNullConstraintResultData(0L)),
      status = ConstraintSuccess
    )
    Check.sqlTable(spark, tableName).addConstraint(constraint).run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "require the table to exist" in {
    intercept[IllegalArgumentException] {
      Check.sqlTable(spark, "doesnotexist").run()
    }
  }

  "A check from a HiveContext" should "load the given table from the given database" in {
    val tableName = "myintegerdf2"
    val databaseName = "testDb"
    spark.sql(s"CREATE DATABASE $databaseName")
    spark.sql(s"USE $databaseName")
    val df = TestData.makeIntegerDf(spark, List(1, 2, 3))
    df.createOrReplaceTempView(tableName)
    spark.sql(s"USE default")
    val columnName = "column"
    val constraint = Check.isNeverNull(columnName)
    val result = NeverNullConstraintResult(
      constraint = NeverNullConstraint(columnName),
      data = Some(NeverNullConstraintResultData(0L)),
      status = ConstraintSuccess
    )
    Check.hiveTable(spark, databaseName, tableName).addConstraint(constraint).run().
      constraintResults shouldBe Map(constraint -> result)
  }

  it should "require the table to exist" in {
    intercept[IllegalArgumentException] {
      Check.hiveTable(spark, "default", "doesnotexist").run()
    }
  }

  "The run method on a Check" should "work correctly when multiple reporters are specified" in {
    val df = mock[DataFrame]
    when(df.toString).thenReturn("")
    when(df.count).thenReturn(1)
    when(df.columns).thenReturn(Array.empty[String])

    val reporter1 = mock[Reporter]
    val reporter2 = mock[Reporter]

    val constraints = Seq.empty[Constraint]
    val check = Check(df, None, None, constraints)
    val result = check.run(reporter1, reporter2)

    result.check shouldBe check
    result.constraintResults shouldBe Map.empty

    verify(reporter1).report(result)
    verify(reporter2).report(result)
  }

  it should "work correctly when a single reporter is specified" in {
    val df = mock[DataFrame]
    when(df.toString).thenReturn("")
    when(df.count).thenReturn(1)
    when(df.columns).thenReturn(Array.empty[String])

    val reporter = mock[Reporter]

    val constraints = Seq.empty[Constraint]
    val check = Check(df, None, None, constraints)
    val result = check.run(reporter)

    result.check shouldBe check
    result.constraintResults shouldBe Map.empty

    verify(reporter).report(result)
  }

  it should "use the console reporter if no reporter is specified" in {
    val df = mock[DataFrame]
    when(df.toString).thenReturn("")
    when(df.count).thenReturn(1)
    when(df.columns).thenReturn(Array.empty[String])

    val defaultBaos = new ByteArrayOutputStream()
    val oldOut = Console.out
    Console.setOut(new PrintStream(defaultBaos))

    val consoleBaos = new ByteArrayOutputStream()
    val consoleReporter = new ConsoleReporter(new PrintStream(consoleBaos))

    val constraints = Seq.empty[Constraint]
    val check = Check(df, None, None, constraints)
    val result = check.run()
    check.run(consoleReporter)

    result.check shouldBe check
    result.constraintResults shouldBe Map.empty
    defaultBaos.toString shouldBe consoleBaos.toString

    // reset Console.out
    Console.setOut(oldOut)
  }

}
