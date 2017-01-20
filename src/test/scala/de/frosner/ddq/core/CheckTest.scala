package de.frosner.ddq.core

import java.io.{ByteArrayOutputStream, FileDescriptor, FileOutputStream, PrintStream}

import de.frosner.ddq.constraints._
import de.frosner.ddq.reporters.{ConsoleReporter, Reporter}
import de.frosner.ddq.testutils.{DummyConstraintResult, DummyConstraint, SparkContexts, TestData}
import org.apache.spark.sql.DataFrame
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}

class CheckTest extends FlatSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with MockitoSugar with SparkContexts {

  override def afterAll(): Unit = {
    hive.reset()
  }

  "Multiple checks" should "produce a constraintResults map with all constraints and corresponding results" in {
    val constraintString = "column > 0"
    val columnName = "column"
    val constraint1 = DummyConstraint("1", ConstraintSuccess)
    val constraint2 = DummyConstraint("2", ConstraintFailure)
    val constraint3 = DummyConstraint("3", ConstraintError(new IllegalArgumentException("fail")))
    val check = Check(TestData.makeIntegerDf(sql, List(1,2,3)))
      .addConstraint(constraint1)
      .addConstraint(constraint2)
      .addConstraint(constraint3)

    check.run().constraintResults shouldBe Map(
      constraint1 -> DummyConstraintResult(
        constraint = constraint1,
        message = constraint1.message,
        status = constraint1.status
      ),
      constraint2 -> DummyConstraintResult(
        constraint = constraint2,
        message = constraint2.message,
        status = constraint2.status
      ),
      constraint3 -> DummyConstraintResult(
        constraint = constraint3,
        message = constraint3.message,
        status = constraint3.status
      )
    )
  }

  "A check from a SQLContext" should "load the given table" in {
    val df = TestData.makeIntegerDf(sql, List(1,2,3))
    val tableName = "myintegerdf1"
    df.registerTempTable(tableName)
    val columnName = "column"
    val constraint = Check.isNeverNull(columnName)
    val result = NeverNullConstraintResult(
      constraint = NeverNullConstraint(columnName),
      data = Some(NeverNullConstraintResultData(0L)),
      status = ConstraintSuccess
    )
    Check.sqlTable(sql, tableName).addConstraint(constraint).run().constraintResults shouldBe Map(constraint -> result)
  }

  it should "require the table to exist" in {
    intercept[IllegalArgumentException] {
      Check.sqlTable(sql, "doesnotexist").run()
    }
  }

  "A check from a HiveContext" should "load the given table from the given database" in {
    val tableName = "myintegerdf2"
    val databaseName = "testDb"
    hive.sql(s"CREATE DATABASE $databaseName")
    hive.sql(s"USE $databaseName")
    val df = TestData.makeIntegerDf(hive, List(1,2,3))
    df.registerTempTable(tableName)
    hive.sql(s"USE default")
    val columnName = "column"
    val constraint = Check.isNeverNull(columnName)
    val result = NeverNullConstraintResult(
      constraint = NeverNullConstraint(columnName),
      data = Some(NeverNullConstraintResultData(0L)),
      status = ConstraintSuccess
    )
    Check.hiveTable(hive, databaseName, tableName).addConstraint(constraint).run().
      constraintResults shouldBe Map(constraint -> result)
    hive.sql(s"DROP DATABASE $databaseName")
  }

  it should "require the table to exist" in {
    intercept[IllegalArgumentException] {
      Check.hiveTable(hive, "default", "doesnotexist").run()
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
