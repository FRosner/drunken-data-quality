package de.frosner.ddq.reporters

import java.util.Date

import de.frosner.ddq.constraints._
import de.frosner.ddq.core.{Check, CheckResult}

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
    override def toString = {
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

  private[reporters] def constraintResultToJson[T <: Constraint](checkResult: CheckResult, constraintResult: ConstraintResult[T], date: Date): JSONObject = {
    val check = checkResult.check
    val constraintType = constraintResult.constraint.getClass.getSimpleName.replace("$", "")
    val constraintStatus = constraintResult.status.stringValue
    val constraintMessage = constraintResult.message
    val constraintSpecificFields = constraintResult match {
      case AlwaysNullConstraintResult(AlwaysNullConstraint(column), nonNullRows, status) => Map(
        columnKey -> column,
        failedInstancesKey -> nonNullRows
      )
      case AnyOfConstraintResult(AnyOfConstraint(column, allowed), failedRows, status) => Map(
        columnKey -> column,
        "allowed" -> JSONArray(allowed.map(_.toString).toList),
        failedInstancesKey -> failedRows
      )
      case ColumnColumnConstraintResult(ColumnColumnConstraint(column), violatingRows, status) => Map(
        columnKey -> column.toString,
        failedInstancesKey -> violatingRows
      )
      case ConditionalColumnConstraintResult(ConditionalColumnConstraint(statement, implication), violatingRows, status) => Map(
        "statement" -> statement.toString,
        "implication" -> implication.toString,
        failedInstancesKey -> violatingRows
      )
      case DateFormatConstraintResult(DateFormatConstraint(column, format), failedRows, status) => Map(
        columnKey -> column,
        "dateFormat" -> format.toPattern,
        failedInstancesKey -> failedRows
      )
      case ForeignKeyConstraintResult(ForeignKeyConstraint(columns, ref), nonMatchingRows, status) => Map(
        columnsKey -> columnsToJsonArray(columns),
        referenceTableKey -> ref.toString,
        failedInstancesKey -> JSONMaybe(nonMatchingRows)
      )
      case FunctionalDependencyConstraintResult(FunctionalDependencyConstraint(determinantSet, dependentSet), failedRows, status) => Map(
        "determinantSet" -> JSONArray(determinantSet.toList),
        "dependentSet" -> JSONArray(dependentSet.toList),
        failedInstancesKey -> failedRows
      )
      case JoinableConstraintResult(JoinableConstraint(columns, ref), distinctBefore, matchingKeys, status) => Map(
        columnsKey -> columnsToJsonArray(columns),
        referenceTableKey -> ref.toString,
        "distinctBefore" -> distinctBefore,
        "matchingKeys" -> matchingKeys
      )
      case NeverNullConstraintResult(NeverNullConstraint(column), nullRows, status) => Map(
        columnKey -> column,
        failedInstancesKey -> nullRows
      )
      case NumberOfRowsConstraintResult(NumberOfRowsConstraint(expected), actual, status) => Map(
        "expected" -> expected,
        "actual" -> actual
      )
      case RegexConstraintResult(RegexConstraint(column, regex), failedRows, status) => Map(
        columnKey -> column,
        "regex" -> regex,
        failedInstancesKey -> failedRows
      )
      case StringColumnConstraintResult(StringColumnConstraint(constraint), violatingRules, status) => Map(
        columnKey -> constraint,
        failedInstancesKey -> violatingRules
      )
      case TypeConversionConstraintResult(TypeConversionConstraint(column, convertedType), originalType, failedRows, status) => Map(
        columnKey -> column,
        "originalType" -> originalType.toString,
        "convertedType" -> convertedType.toString,
        failedInstancesKey -> failedRows
      )
      case UniqueKeyConstraintResult(UniqueKeyConstraint(columns), numNonUniqueTuples, status) => Map(
        columnsKey -> JSONArray(columns.toList),
        failedInstancesKey -> numNonUniqueTuples
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
      constraintMessageKey -> constraintMessage
    ).++(constraintSpecificFields))
  }

}
