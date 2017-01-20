package de.frosner.ddq.reporters

import java.util.Date

import de.frosner.ddq.constraints._
import de.frosner.ddq.core.CheckResult
import org.apache.log4j.{Level, Logger}

import scala.util.parsing.json.{JSONArray, JSONObject}

case class Log4jReporter(logger: Logger = Logger.getLogger("DDQ"), logLevel: Level = Level.INFO) extends Reporter {

  override def report(checkResult: CheckResult): Unit = {
    val date = new Date()
    for ((constraint, constraintResult) <- checkResult.constraintResults) {
      logger.log(logLevel, Log4jReporter.constraintResultToJson(checkResult, constraintResult, date).toString())
    }
  }

}

object Log4jReporter {

  /*
   Because the JSON library cannot handle nulls... https://issues.scala-lang.org/browse/SI-5092
   So this class will print the correct JSNull, because non-Strings will not be enclosed by
   */
  private[reporters] case class JSONMaybe[T](maybe: Option[T])(implicit val numeric: Numeric[T] = null) {
    override def toString: String = {
      val maybeEnclosingQuotes = if (numeric == null) "\"" else ""
      maybe.map(m => s"$maybeEnclosingQuotes$m$maybeEnclosingQuotes").getOrElse("null")
    }
  }

  private[reporters] val checkKey = "check"
  private[reporters] val checkIdKey = "id"
  private[reporters] val checkTimeKey = "time"
  private[reporters] val checkNameKey = "name"
  private[reporters] val checkNumRowsKey = "rowsTotal"

  private[reporters] val constraintTypeKey = "constraint"
  private[reporters] val constraintStatusKey = "status"
  private[reporters] val constraintExceptionKey = "exception"
  private[reporters] val constraintMessageKey = "message"

  private[reporters] val failedInstancesKey = "failed"
  private[reporters] val columnKey = "column"
  private[reporters] val columnsKey = "columns"
  private[reporters] val referenceTableKey = "referenceTable"

  private def columnsToJsonArray(columns: Seq[(String, String)]) = JSONArray(
    columns.map{ case (baseColumn, referenceColumn) => JSONObject(Map(
      "baseColumn" -> baseColumn,
      "referenceColumn" -> referenceColumn
    ))}.toList
  )

  private[reporters] def constraintResultToJson[T <: Constraint](checkResult: CheckResult,
                                                                 constraintResult: ConstraintResult[T],
                                                                 date: Date): JSONObject = {
    val check = checkResult.check
    val constraintType = constraintResult.constraint.getClass.getSimpleName.replace("$", "")
    val constraintStatus = constraintResult.status.stringValue
    val maybeConstraintException = JSONMaybe(
      constraintResult.status match {
        case ConstraintError(throwable) => Some(throwable.toString)
        case other => None
      }
    )
    val constraintMessage = constraintResult.message
    val constraintSpecificFields = constraintResult match {
      case alwaysNullConstraintResult: AlwaysNullConstraintResult => Map(
        columnKey -> alwaysNullConstraintResult.constraint.columnName,
        failedInstancesKey -> JSONMaybe(alwaysNullConstraintResult.data.map(_.nonNullRows))
      )
      case anyOfConstraintResult: AnyOfConstraintResult => Map(
        columnKey -> anyOfConstraintResult.constraint.columnName,
        "allowed" -> JSONArray(anyOfConstraintResult.constraint.allowedValues.map(_.toString).toList),
        failedInstancesKey -> JSONMaybe(anyOfConstraintResult.data.map(_.failedRows))
      )
      case columnColumnConstraintResult: ColumnColumnConstraintResult => Map(
        columnKey -> columnColumnConstraintResult.constraint.constraintColumn.toString,
        failedInstancesKey -> JSONMaybe(columnColumnConstraintResult.data.map(_.failedRows))
      )
      case conditionalColumnConstraintResult: ConditionalColumnConstraintResult => Map(
        "statement" -> conditionalColumnConstraintResult.constraint.statement.toString,
        "implication" -> conditionalColumnConstraintResult.constraint.implication.toString,
        failedInstancesKey -> JSONMaybe(conditionalColumnConstraintResult.data.map(_.failedRows))
      )
      case dateFormatConstraintResult: DateFormatConstraintResult => Map(
        columnKey -> dateFormatConstraintResult.constraint.columnName,
        "dateFormat" -> dateFormatConstraintResult.constraint.formatString,
        failedInstancesKey -> JSONMaybe(dateFormatConstraintResult.data.map(_.failedRows))
      )
      case ForeignKeyConstraintResult(ForeignKeyConstraint(columns, ref), nonMatchingRows, status) => Map(
        columnsKey -> columnsToJsonArray(columns),
        referenceTableKey -> ref.toString,
        failedInstancesKey -> JSONMaybe(nonMatchingRows.flatMap(_.numNonMatchingRefs))
      )
      case functionalDependencyConstraintResult: FunctionalDependencyConstraintResult => Map(
        "determinantSet" -> JSONArray(functionalDependencyConstraintResult.constraint.determinantSet.toList),
        "dependentSet" -> JSONArray(functionalDependencyConstraintResult.constraint.dependentSet.toList),
        failedInstancesKey -> JSONMaybe(functionalDependencyConstraintResult.data.map(_.failedRows))
      )
      case joinableConstraintResult: JoinableConstraintResult => Map(
        columnsKey -> columnsToJsonArray(joinableConstraintResult.constraint.columnNames),
        referenceTableKey -> joinableConstraintResult.constraint.referenceTable.toString,
        "distinctBefore" -> JSONMaybe(joinableConstraintResult.data.map(_.distinctBefore)),
        "matchingKeys" -> JSONMaybe(joinableConstraintResult.data.map(_.matchingKeys))
      )
      case neverNullConstraintResult: NeverNullConstraintResult => Map(
        columnKey -> neverNullConstraintResult.constraint.columnName,
        failedInstancesKey -> JSONMaybe(neverNullConstraintResult.data.map(_.nullRows))
      )
      case numberOfRowsConstraintResult: NumberOfRowsConstraintResult => Map(
        "expected" -> numberOfRowsConstraintResult.constraint.expected.toString,
        "actual" -> numberOfRowsConstraintResult.actual
      )
      case regexConstraintResult: RegexConstraintResult => Map(
        columnKey -> regexConstraintResult.constraint.columnName,
        "regex" -> regexConstraintResult.constraint.regex,
        failedInstancesKey -> JSONMaybe(regexConstraintResult.data.map(_.failedRows))
      )
      case stringColumnConstraintResult: StringColumnConstraintResult => Map(
        columnKey -> stringColumnConstraintResult.constraint.constraintString,
        failedInstancesKey -> JSONMaybe(stringColumnConstraintResult.data.map(_.failedRows))
      )
      case typeConversionConstraintResult: TypeConversionConstraintResult => Map(
        columnKey -> typeConversionConstraintResult.constraint.columnName,
        "originalType" -> JSONMaybe(typeConversionConstraintResult.data.map(_.originalType.toString)),
        "convertedType" -> typeConversionConstraintResult.constraint.convertedType.toString,
        failedInstancesKey -> JSONMaybe(typeConversionConstraintResult.data.map(_.failedRows))
      )
      case uniqueKeyConstraint: UniqueKeyConstraintResult => Map(
        columnsKey -> JSONArray(uniqueKeyConstraint.constraint.columnNames.toList),
        failedInstancesKey -> JSONMaybe(uniqueKeyConstraint.data.map(_.numNonUniqueTuples))
      )
      case other => Map.empty[String, Any]
    }
    JSONObject(Map(
      checkKey -> JSONObject(Map(
        checkIdKey -> check.id,
        checkTimeKey -> date.toString,
        checkNameKey -> check.name,
        checkNumRowsKey -> checkResult.numRows
      )),
      constraintTypeKey -> constraintType,
      constraintStatusKey -> constraintStatus,
      constraintExceptionKey -> maybeConstraintException,
      constraintMessageKey -> constraintMessage
    ).++(constraintSpecificFields))
  }

}
