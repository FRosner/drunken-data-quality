package de.frosner.ddq.reporters

import java.text.SimpleDateFormat
import java.util.Date

import de.frosner.ddq.constraints._
import de.frosner.ddq.core._
import de.frosner.ddq.reporters.Log4jReporter.JSONMaybe
import de.frosner.ddq.testutils.DummyConstraint
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{Column, DataFrame}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}

import scala.util.parsing.json.{JSONArray, JSONObject}

class Log4jReporterTest extends FlatSpec with Matchers with MockitoSugar {

  private def checkJsonOf[T <: Constraint](constraintResult: ConstraintResult[T],
                                           additionalExpectedColumns: Map[String, Any]) = {
    val constraint = constraintResult.constraint
    val checkId = "check"
    val checkName = "df"
    val checkNumRows = 5L
    val check = Check(
      dataFrame = mock[DataFrame],
      displayName = Some(checkName),
      cacheMethod = None,
      constraints = Seq(constraint),
      id = checkId
    )
    val checkTime = new Date()
    val checkResult = CheckResult(Map(constraint -> constraintResult), check, checkNumRows)

    Log4jReporter.constraintResultToJson(checkResult, constraintResult, checkTime) shouldBe JSONObject(
      Map(
        Log4jReporter.checkKey -> JSONObject(Map(
          Log4jReporter.checkIdKey -> checkId,
          Log4jReporter.checkTimeKey -> checkTime.toString,
          Log4jReporter.checkNameKey -> checkName,
          Log4jReporter.checkNumRowsKey -> checkNumRows
        )),
        Log4jReporter.constraintTypeKey -> constraint.getClass.getSimpleName.replace("$", ""),
        Log4jReporter.constraintStatusKey -> constraintResult.status.stringValue,
        Log4jReporter.constraintExceptionKey -> JSONMaybe(
          constraintResult.status match {
            case ConstraintError(throwable) => Some(throwable.toString)
            case other => None
          }
        ),
        Log4jReporter.constraintMessageKey -> constraintResult.message
      ) ++ additionalExpectedColumns
    )
  }

  "JSON serialization" should "work for AlwaysNullConstraintResult" in {
    val columnName = "c"
    val nonNullRows = 5L
    val constraintResult = AlwaysNullConstraintResult(
      constraint = AlwaysNullConstraint(columnName),
      status = ConstraintFailure,
      data = Some(AlwaysNullConstraintResultData(nonNullRows))
    )

    checkJsonOf(constraintResult, Map(
      Log4jReporter.columnKey -> columnName,
      Log4jReporter.failedInstancesKey -> JSONMaybe(Some(nonNullRows))
    ))
  }

  it should "work for AnyOfConstraintResult" in {
    val columnName = "d"
    val allowedValues = Set("a", 5)
    val failedRows = 0L
    val constraintResult = AnyOfConstraintResult(
      constraint = AnyOfConstraint(columnName, allowedValues),
      status = ConstraintSuccess,
      data = Some(AnyOfConstraintResultData(failedRows = failedRows))
    )

    checkJsonOf(constraintResult, Map(
      Log4jReporter.columnKey -> columnName,
      Log4jReporter.failedInstancesKey -> JSONMaybe(Some(failedRows)),
      "allowed" -> JSONArray(List("a", "5"))
    ))
  }

  it should "work for ColumnColumnConstraintResult" in {
    val constraint = new Column("c") > 5
    val failedRows = 0L
    val constraintResult = ColumnColumnConstraintResult(
      constraint = ColumnColumnConstraint(constraint),
      status = ConstraintSuccess,
      data = Some(ColumnColumnConstraintResultData(failedRows))
    )

    checkJsonOf(constraintResult, Map(
      Log4jReporter.columnKey -> constraint.toString,
      Log4jReporter.failedInstancesKey -> JSONMaybe(Some(failedRows))
    ))
  }

  it should "work for ConditionalColumnConstraintResult" in {
    val statement = new Column("c") > 5
    val implication = new Column("d") > 6
    val failedRows = 0L
    val constraintResult = ConditionalColumnConstraintResult(
      constraint = ConditionalColumnConstraint(statement, implication),
      status = ConstraintSuccess,
      data = Some(ConditionalColumnConstraintResultData(failedRows = failedRows))
    )

    checkJsonOf(constraintResult, Map(
      "statement" -> statement.toString,
      "implication" -> implication.toString,
      Log4jReporter.failedInstancesKey -> JSONMaybe(Some(failedRows))
    ))
  }

  it should "work for DateFormatConstraintResult" in {
    val column = "c"
    val formatString = "yyyy"
    val format = new SimpleDateFormat(formatString)
    val failedRows = 1L
    val constraintResult = DateFormatConstraintResult(
      constraint = DateFormatConstraint(column, format),
      status = ConstraintFailure,
      data = Some(DateFormatConstraintResultData(failedRows = failedRows))
    )

    checkJsonOf(constraintResult, Map(
      Log4jReporter.columnKey -> column,
      Log4jReporter.failedInstancesKey -> JSONMaybe(Some(failedRows)),
      "dateFormat" -> formatString
    ))
  }

  it should "work for ForeignKeyConstraintResult (failed rows)" in {
    val columns = Seq("c" -> "d")
    val failedRows = 1L
    val df = mock[DataFrame]
    val dfName = "df"
    when(df.toString).thenReturn(dfName)

    val constraintResult = ForeignKeyConstraintResult(
      constraint = ForeignKeyConstraint(columns, df),
      status = ConstraintFailure,
      data = Some(ForeignKeyConstraintResultData(Some(failedRows)))
    )

    checkJsonOf(constraintResult, Map(
      Log4jReporter.columnsKey -> JSONArray(List(
        JSONObject(Map("baseColumn" -> "c", "referenceColumn" -> "d"))
      )),
      "referenceTable" -> dfName,
      Log4jReporter.failedInstancesKey -> JSONMaybe(Some(failedRows))
    ))
  }

  it should "work for ForeignKeyConstraintResult (no failed rows)" in {
    val columns = Seq("c" -> "d")
    val df = mock[DataFrame]
    val dfName = "df"
    when(df.toString).thenReturn(dfName)

    val constraintResult = ForeignKeyConstraintResult(
      constraint = ForeignKeyConstraint(columns, df),
      status = ConstraintFailure,
      data = Some(ForeignKeyConstraintResultData(None))
    )

    checkJsonOf(constraintResult, Map(
      Log4jReporter.columnsKey -> JSONArray(List(
        JSONObject(Map("baseColumn" -> "c", "referenceColumn" -> "d"))
      )),
      "referenceTable" -> dfName,
      Log4jReporter.failedInstancesKey -> JSONMaybe(Option.empty[Long])
    ))
  }

  it should "work for ForeignKeyConstraintResult (error)" in {
    val columns = Seq("c" -> "d")
    val df = mock[DataFrame]
    val dfName = "df"
    when(df.toString).thenReturn(dfName)

    val constraintResult = ForeignKeyConstraintResult(
      constraint = ForeignKeyConstraint(columns, df),
      status = ConstraintError(new IllegalArgumentException()),
      data = None
    )

    checkJsonOf(constraintResult, Map(
      Log4jReporter.columnsKey -> JSONArray(List(
        JSONObject(Map("baseColumn" -> "c", "referenceColumn" -> "d"))
      )),
      "referenceTable" -> dfName,
      Log4jReporter.failedInstancesKey -> JSONMaybe(Option.empty[Long])
    ))
  }

  it should "work for FunctionalDependencyConstraintResult" in {
    val determinantSet = Seq("a", "b")
    val dependentSet = Seq("c", "d")
    val failedRows = 1L
    val constraintResult = FunctionalDependencyConstraintResult(
      constraint = FunctionalDependencyConstraint(determinantSet, dependentSet),
      status = ConstraintFailure,
      data = Some(FunctionalDependencyConstraintResultData(failedRows = failedRows))
    )

    checkJsonOf(constraintResult, Map(
      "determinantSet" -> JSONArray(List("a", "b")),
      "dependentSet" -> JSONArray(List("c", "d")),
      Log4jReporter.failedInstancesKey -> JSONMaybe(Some(failedRows))
    ))
  }

  it should "work for JoinableConstraintResult" in {
    val columns = Seq("c" -> "d")
    val formatString = "yyyy"
    val format = new SimpleDateFormat(formatString)
    val failedRows = 1L
    val df = mock[DataFrame]
    val dfName = "df"
    when(df.toString).thenReturn(dfName)
    val constraintResult = JoinableConstraintResult(
      constraint = JoinableConstraint(columns, df),
      data = Some(JoinableConstraintResultData(
        distinctBefore = 5L,
        matchingKeys = 3L
      )),
      status = ConstraintFailure
    )

    checkJsonOf(constraintResult, Map(
      Log4jReporter.columnsKey -> JSONArray(List(
        JSONObject(Map("baseColumn" -> "c", "referenceColumn" -> "d"))
      )),
      "referenceTable" -> dfName,
      "distinctBefore" -> JSONMaybe(Some(5L)),
      "matchingKeys" -> JSONMaybe(Some(3L))
    ))
  }

  it should "work for NeverNullConstraintResult" in {
    val column = "c"
    val failedRows = 1L
    val constraintResult = NeverNullConstraintResult(
      constraint = NeverNullConstraint(column),
      status = ConstraintFailure,
      data = Some(NeverNullConstraintResultData(failedRows))
    )

    checkJsonOf(constraintResult, Map(
      Log4jReporter.columnKey -> column,
      Log4jReporter.failedInstancesKey -> JSONMaybe(Some(failedRows))
    ))
  }

  it should "work for NumberOfRowsConstraintResult" in {
    val expected = new Column("count") === 1L
    val actual = 1L
    val constraintResult = NumberOfRowsConstraintResult(
      constraint = NumberOfRowsConstraint(expected),
      status = ConstraintSuccess,
      actual = actual
    )

    checkJsonOf(constraintResult, Map(
      "expected" -> expected.toString,
      "actual" -> actual
    ))
  }

  it should "work for RegexConstraintResult" in {
    val column = "c"
    val regex = "reg"
    val failedRows = 0L
    val constraintResult = RegexConstraintResult(
      constraint = RegexConstraint(column, regex),
      status = ConstraintSuccess,
      data = Some(RegexConstraintResultData(failedRows = failedRows))
    )

    checkJsonOf(constraintResult, Map(
      Log4jReporter.columnKey -> column,
      Log4jReporter.failedInstancesKey -> JSONMaybe(Some(failedRows)),
      "regex" -> regex
    ))
  }

  it should "work for StringColumnConstraintResult" in {
    val column = "c"
    val failedRows = 0L
    val constraintResult = StringColumnConstraintResult(
      constraint = StringColumnConstraint(column),
      status = ConstraintSuccess,
      data = Some(StringColumnConstraintResultData(failedRows = failedRows))
    )

    checkJsonOf(constraintResult, Map(
      Log4jReporter.columnKey -> column,
      Log4jReporter.failedInstancesKey -> JSONMaybe(Some(failedRows))
    ))
  }

  it should "work for TypeConversionConstraintResult" in {
    val column = "c"
    val failedRows = 0L
    val constraintResult = TypeConversionConstraintResult(
      constraint = TypeConversionConstraint(column, StringType),
      status = ConstraintSuccess,
      failedRows = failedRows,
      originalType = IntegerType
    )

    checkJsonOf(constraintResult, Map(
      Log4jReporter.columnKey -> column,
      Log4jReporter.failedInstancesKey -> failedRows,
      "originalType" -> "IntegerType",
      "convertedType" -> "StringType"
    ))
  }

  it should "work for UniqueKeyConstraintResult" in {
    val columns = Seq("c", "d")
    val failedRows = 0L
    val constraintResult = UniqueKeyConstraintResult(
      constraint = UniqueKeyConstraint(columns),
      status = ConstraintSuccess,
      numNonUniqueTuples = failedRows
    )

    checkJsonOf(constraintResult, Map(
      Log4jReporter.columnsKey -> JSONArray(List("c", "d")),
      Log4jReporter.failedInstancesKey -> failedRows
    ))
  }

  "A log4j reporter" should "log correctly to the default logger with the default log level" in {
    val log4jReporter = Log4jReporter()

    val df = mock[DataFrame]
    val dfName = "myDf"
    val numRows = 10

    val result1 = ForeignKeyConstraintResult(
      constraint = ForeignKeyConstraint(Seq(("a", "b")), df),
      data = Some(ForeignKeyConstraintResultData(None)),
      status = ConstraintFailure
    )
    val result2 = ForeignKeyConstraintResult(
      constraint = ForeignKeyConstraint(Seq(("a", "b"), ("c", "d")), df),
      data = Some(ForeignKeyConstraintResultData(Some(5L))),
      status = ConstraintFailure
    )

    val constraints = Map[Constraint, ConstraintResult[Constraint]](
      result1.constraint -> result1,
      result2.constraint -> result2
    )
    val check = Check(df, Some(dfName), Option.empty, constraints.keys.toSeq)

    log4jReporter.report(CheckResult(constraints, check, numRows))
  }

  it should "log the JSON format of a constraint result using the specified logger and log level" in {
    val logger = mock[Logger]
    val logLevel = Level.ERROR
    val log4jReporter = Log4jReporter(logger, logLevel)

    val df = mock[DataFrame]
    val dfName = "myDf"
    val numRows = 10
    val id = "a"

    val message1 = "1"
    val status1 = ConstraintSuccess
    val constraint1 = DummyConstraint(message1, status1)
    val result1 = constraint1.fun(df)

    val constraints = Map[Constraint, ConstraintResult[Constraint]](
      result1.constraint -> result1
    )
    val check = Check(df, Some(dfName), Option.empty, constraints.keys.toSeq, id)

    val date = new Date()
    log4jReporter.report(CheckResult(constraints, check, numRows))

    verify(logger).log(logLevel, s"""{"exception" : null, "check" : {"id" : "$id", "time" : "$date", "name" : "$dfName", "rowsTotal" : $numRows}, "status" : "Success", "constraint" : "DummyConstraint", "message" : "$message1"}""")
  }

}
