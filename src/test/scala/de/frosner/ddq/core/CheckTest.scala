package de.frosner.ddq.core

import java.io.{FileOutputStream, FileDescriptor, PrintStream, ByteArrayOutputStream}
import java.text.SimpleDateFormat

import de.frosner.ddq.constraints._
import de.frosner.ddq.testutils.TestData
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SQLContext}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}

import de.frosner.ddq.reporters.{ConsoleReporter, Reporter}

class CheckTest extends FlatSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with MockitoSugar {

  private val hive = TestHive
  hive.setConf("spark.sql.shuffle.partitions", "5")
  private val sc = hive.sparkContext
  private val sql = new SQLContext(sc)
  sql.setConf("spark.sql.shuffle.partitions", "5")

  override def afterAll(): Unit = {
    hive.reset()
  }


  "Multiple checks" should "produce a constraintResults map with all constraints and corresponding results" in {
    val expectedNumberOfRows1 = 3
    val expectedNumberOfRows2 = 2
    val constraintString = "column > 0"
    val columnName = "column"
    val check = Check(TestData.makeIntegerDf(sql, List(1,2,3)))
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
        nullRows = 0L,
        status = ConstraintSuccess
      ),
      constraint3 -> StringColumnConstraintResult(
        constraint = StringColumnConstraint(constraintString),
        violatingRows = 0L,
        status = ConstraintSuccess
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
      nullRows = 0L,
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
      nullRows = 0L,
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
    System.setOut(new PrintStream(defaultBaos))

    val consoleBaos = new ByteArrayOutputStream()
    val consoleReporter = new ConsoleReporter(new PrintStream(consoleBaos))

    val constraints = Seq.empty[Constraint]
    val check = Check(df, None, None, constraints)
    val result = check.run()
    check.run(consoleReporter)

    result.check shouldBe check
    result.constraintResults shouldBe Map.empty
    defaultBaos.toString shouldBe consoleBaos.toString

    // reset System.out
    System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)))
  }

}
