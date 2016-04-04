package de.frosner.ddq.core

import java.text.SimpleDateFormat
import java.util.UUID

import de.frosner.ddq.constraints._
import de.frosner.ddq.reporters.{ConsoleReporter, Reporter}
import de.frosner.ddq.{constraints, core}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel

import scala.util.Try

/**
 * A class representing a list of constraints that can be applied to a given [[org.apache.spark.sql.DataFrame]].
 * In order to run the checks, use the `run` method.
 *
 * @param dataFrame The table to check
 * @param displayName The name to show in the logs. If it is not set, `toString` will be used.
 * @param cacheMethod The [[org.apache.spark.storage.StorageLevel]] to persist with before executing the checks.
 *                    If it is not set, no persisting will be attempted
 * @param constraints The constraints to apply when this check is run. New ones can be added and will return a new object
 */
case class Check(dataFrame: DataFrame,
                 displayName: Option[String] = Option.empty,
                 cacheMethod: Option[StorageLevel] = Check.defaultCacheMethod,
                 constraints: Seq[Constraint] = Seq.empty,
                 id: String = UUID.randomUUID.toString) {

  val name = displayName.getOrElse(dataFrame.toString)

  def addConstraint(c: Constraint): Check =
    Check(dataFrame, displayName, cacheMethod, constraints ++ List(c))

  /**
   * Check whether the given columns are a unique key for this table.
   *
   * @param columnName name of the first column that is supposed to be part of the unique key
   * @param columnNames names of the other columns that are supposed to be part of the unique key
   * @return [[core.Check]] object including this constraint
   */
  def hasUniqueKey(columnName: String, columnNames: String*): Check = addConstraint(Check.hasUniqueKey(columnName, columnNames:_*))

  /**
   * Check whether the given constraint is satisfied. The constraint has to comply with Spark SQL syntax. So you
   * can just write it the same way that you would put it inside a `WHERE` clause.
   *
   * @param constraint The constraint that needs to be satisfied for all columns
   * @return [[core.Check]] object including this constraint
   */
  def satisfies(constraint: String): Check = addConstraint(Check.satisfies(constraint))

  /**
    * Check whether the given constraint is satisfied. The constraint is built using the
    * [[org.apache.spark.sql.Column]] class.
    *
    * @param constraint The constraint that needs to be satisfied for all columns
    * @return [[core.Check]] object including this constraint
   */
  def satisfies(constraint: Column): Check = addConstraint(Check.satisfies(constraint))

  /**
   * <p>Check whether the given conditional constraint is satisfied. The constraint is built using the
   * [[org.apache.spark.sql.Column]] class.</p><br/>
   * Usage:
   * {{{
   * Check(df).satisfies((new Column("c1") === 1) -> (new Column("c2").isNotNull))
   * }}}
   *
   * @param conditional The constraint that needs to be satisfied for all columns
   * @return [[core.Check]] object including this constraint
   */
  def satisfies(conditional: (Column, Column)): Check = addConstraint(Check.satisfies(conditional))

  /**
   * Check whether the column with the given name contains no null values.
   *
   * @param columnName Name of the column to check
   * @return [[core.Check]] object including this constraint
   */
  def isNeverNull(columnName: String): Check = addConstraint(Check.isNeverNull(columnName))

  /**
   * Check whether the column with the given name contains only null values.
   *
   * @param columnName Name of the column to check
   * @return [[core.Check]] object including this constraint
   */
  def isAlwaysNull(columnName: String): Check = addConstraint(Check.isAlwaysNull(columnName))

  /**
   * Check whether the table has exactly the given number of rows.
   *
   * @param expected Expected number of rows.
   * @return [[core.Check]] object including this constraint
   */
  def hasNumRows(expected: Column => Column): Check = addConstraint(Check.hasNumRowsEqualTo(expected))

  /**
    * Check whether the column with the given name can be converted to the given type.
    *
    * @param columnName Name of the column to check
    * @param targetType Type to try to convert to
    * @return [[core.Check]] object including this constraint
    */
  def isConvertibleTo(columnName: String, targetType: DataType): Check =
    addConstraint(Check.isConvertibleTo(columnName, targetType))

  /**
   * Check whether the column with the given name can be converted to a date using the specified date format.
   *
   * @param columnName Name of the column to check
   * @param dateFormat Date format to use for conversion
   * @return [[core.Check]] object including this constraint
   */
  def isFormattedAsDate(columnName: String, dateFormat: SimpleDateFormat): Check = addConstraint(
    Check.isFormattedAsDate(columnName, dateFormat))

  /**
   * Check whether the column with the given name is always any of the specified values.
   *
   * @param columnName Name of the column to check
   * @param allowed Set of allowed values
   * @return [[core.Check]] object including this constraint
   */
  def isAnyOf(columnName: String, allowed: Set[Any]): Check = addConstraint(Check.isAnyOf(columnName, allowed))

  /**
   * Check whether the column with the given name is always matching the specified regular expression.
   *
   * @param columnName Name of the column to check
   * @param regex Regular expression that needs to match
   * @return [[core.Check]] object including this constraint
   */
  def isMatchingRegex(columnName: String, regex: String): Check = addConstraint(Check.isMatchingRegex(columnName, regex))

  /**
   * Check whether the columns with the given names define a foreign key to the specified reference table.
   *
   * @param referenceTable Table to which the foreign key is pointing
   * @param keyMap Column mapping from this table to the reference one (`"column1" -> "base_column1"`)
   * @param keyMaps Column mappings from this table to the reference one (`"column1" -> "base_column1"`)
   * @return [[core.Check]] object including this constraint
   */
  def hasForeignKey(referenceTable: DataFrame, keyMap: (String, String), keyMaps: (String, String)*): Check = addConstraint(
    Check.hasForeignKey(referenceTable, keyMap, keyMaps: _*)
  )

  /**
   * Check whether a join between this table and the given reference table returns any results. This can be seen
   * as a weaker version of the foreign key check, as it requires only partial matches.
   *
   * @param referenceTable Table to join with
   * @param keyMap Column mapping from this table to the reference one (`"column1" -> "base_column1"`)
   * @param keyMaps Column mappings from this table to the reference one (`"column1" -> "base_column1"`)
   * @return [[core.Check]] object including this constraint
   */
  def isJoinableWith(referenceTable: DataFrame, keyMap: (String, String), keyMaps: (String, String)*): Check = addConstraint(
    Check.isJoinableWith(referenceTable, keyMap, keyMaps: _*)
  )

  /**
   * Check whether the columns in the dependent set have a functional dependency on determinant set.
   *
   * @param determinantSet sequence of column names which form a determinant set
   * @param dependentSet sequence of column names which form a dependent set
   * @return [[core.Check]] object including this constraint
   */
  def hasFunctionalDependency(determinantSet: Seq[String], dependentSet: Seq[String]): Check = addConstraint(
    Check.hasFunctionalDependency(determinantSet, dependentSet)
  )

  /**
   * Run check with all the previously specified constraints and report to every reporter passed as an argument
   *
   * @param reporters iterable of reporters to produce output on the check result
   * @return check result
   **/
  def run(reporters: Reporter*): CheckResult = {
    val actualReporters = if (reporters.isEmpty) List(ConsoleReporter()) else reporters
    Runner.run(List(this), actualReporters)(this)
  }

}

object Check {

  private val defaultCacheMethod = Option(StorageLevel.MEMORY_ONLY)

  /**
   * Construct a check object using the given [[org.apache.spark.sql.SQLContext]] and table name.
   *
   * @param sql SQL context to read the table from
   * @param table Name of the table to check
   * @param cacheMethod The [[org.apache.spark.storage.StorageLevel]] to persist with before executing the checks.
   *                    If it is not set, no persisting will be attempted
   * @return Check object that can be applied on the given table
   */
  def sqlTable(sql: SQLContext,
               table: String,
               cacheMethod: Option[StorageLevel] = defaultCacheMethod): Check = {
    val tryTable = Try(sql.table(table))
    require(tryTable.isSuccess, s"""Failed to reference table $table: ${tryTable.failed.getOrElse("No exception provided")}""")
    Check(
      dataFrame = tryTable.get,
      displayName = Option(table),
      cacheMethod = cacheMethod
    )
  }

  /**
   * Check whether the given columns are a unique key for this table.
   *
   * @param columnName name of the first column that is supposed to be part of the unique key
   * @param columnNames names of the other columns that are supposed to be part of the unique key
   * @return [[Constraint]] object
   */
  def hasUniqueKey(columnName: String, columnNames: String*): Constraint =
    UniqueKeyConstraint(columnName :: columnNames.toList)

  /**
   * Check whether the table has exactly the given number of rows.
   *
   * @param expected Expected number of rows.
   * @return [[constraints.Constraint]] object
   */
  def hasNumRowsEqualTo(expected: Column => Column): Constraint = NumberOfRowsConstraint(expected)

  /**
   * Check whether the given constraint is satisfied. The constraint has to comply with Spark SQL syntax. So you
   * can just write it the same way that you would put it inside a `WHERE` clause.
   *
   * @param constraint The constraint that needs to be satisfied for all columns
   * @return [[constraints.Constraint]] object
   */
  def satisfies(constraint: String): Constraint = StringColumnConstraint(constraint)

  /**
   * Check whether the given constraint is satisfied. The constraint is built using the
   * [[org.apache.spark.sql.Column]] class.
   *
   * @param constraint The constraint that needs to be satisfied for all columns
   * @return [[constraints.Constraint]] object
   */
  def satisfies(constraint: Column): Constraint = ColumnColumnConstraint(constraint)

  /**
   * <p>Check whether the given conditional constraint is satisfied. The constraint is built using the
   * [[org.apache.spark.sql.Column]] class.</p><br/>
   * Usage:
   * {{{
   * Check(df).satisfies((new Column("c1") === 1) -> (new Column("c2").isNotNull))
   * }}}
   *
   * @param conditional The constraint that needs to be satisfied for all columns
   * @return [[constraints.Constraint]] object
   */
  def satisfies(conditional: (Column, Column)): Constraint = {
    val (statement, implication) = conditional
    ConditionalColumnConstraint(statement, implication)
  }

  /**
   * Check whether the column with the given name contains only null values.
   *
   * @param columnName Name of the column to check
   * @return [[constraints.Constraint]] object
   */
  def isAlwaysNull(columnName: String): Constraint = AlwaysNullConstraint(columnName)

  /**
   * Check whether the column with the given name contains no null values.
   *
   * @param columnName Name of the column to check
   * @return [[constraints.Constraint]] object
   */
  def isNeverNull(columnName: String): Constraint = NeverNullConstraint(columnName)

  /**
    * Check whether the column with the given name can be converted to the given type.
    *
    * @param columnName Name of the column to check
    * @param targetType Type to try to convert to
    * @return [[constraints.Constraint]] object
    */
  def isConvertibleTo(columnName: String, targetType: DataType): Constraint =
    TypeConversionConstraint(columnName, targetType)

  /**
   * Check whether the column with the given name can be converted to a date using the specified date format.
   *
   * @param columnName Name of the column to check
   * @param dateFormat Date format to use for conversion
   * @return [[constraints.Constraint]] object
   */
  def isFormattedAsDate(columnName: String, dateFormat: SimpleDateFormat): Constraint =
    DateFormatConstraint(columnName, dateFormat)

  /**
   * Check whether the column with the given name is always any of the specified values.
   *
   * @param columnName Name of the column to check
   * @param allowed Set of allowed values
   * @return [[constraints.Constraint]] object
   */
  def isAnyOf(columnName: String, allowed: Set[Any]): Constraint = AnyOfConstraint(columnName, allowed)

  /**
   * Check whether the column with the given name is always matching the specified regular expression.
   *
   * @param columnName Name of the column to check
   * @param regex Regular expression that needs to match
   * @return [[constraints.Constraint]] object
   */
  def isMatchingRegex(columnName: String, regex: String): Constraint = RegexConstraint(columnName, regex)

  /**
   * Check whether the columns with the given names define a foreign key to the specified reference table.
   *
   * @param referenceTable Table to which the foreign key is pointing
   * @param keyMap Column mapping from this table to the reference one (`"column1" -> "base_column1"`)
   * @param keyMaps Column mappings from this table to the reference one (`"column1" -> "base_column1"`)
   * @return [[constraints.Constraint]] object
   */
  def hasForeignKey(referenceTable: DataFrame, keyMap: (String, String), keyMaps: (String, String)*): Constraint = {
    val columns = keyMap :: keyMaps.toList
    ForeignKeyConstraint(columns, referenceTable)
  }

  /**
   * Check whether a join between this table and the given reference table returns any results. This can be seen
   * as a weaker version of the foreign key check, as it requires only partial matches.
   *
   * @param referenceTable Table to join with
   * @param keyMap Column mapping from this table to the reference one (`"column1" -> "base_column1"`)
   * @param keyMaps Column mappings from this table to the reference one (`"column1" -> "base_column1"`)
   * @return [[constraints.Constraint]] object
   */
  def isJoinableWith(referenceTable: DataFrame, keyMap: (String, String), keyMaps: (String, String)*): Constraint = {
    val columns = keyMap :: keyMaps.toList
    JoinableConstraint(columns, referenceTable)
  }

  /**
   * Check whether the columns in the dependent set have a functional dependency on determinant set.
   *
   * @param determinantSet sequence of column names which form a determinant set
   * @param dependentSet sequence of column names which form a dependent set
   * @return [[constraints.Constraint]] object
   */
  def hasFunctionalDependency(determinantSet: Seq[String], dependentSet: Seq[String]): Constraint =
    FunctionalDependencyConstraint(determinantSet, dependentSet)


  /**
   * Construct a check object using the given [[org.apache.spark.sql.SQLContext]] and table name.
   *
   * @param hive Hive context to read the table from
   * @param database Database to switch to before attempting to read the table
   * @param table Name of the table to check
   * @param cacheMethod The [[org.apache.spark.storage.StorageLevel]] to persist with before executing the checks.
   *                    If it is not set, no persisting will be attempted
   * @return Check object that can be applied on the given table
   */
  def hiveTable(hive: HiveContext,
                database: String,
                table: String,
                cacheMethod: Option[StorageLevel] = defaultCacheMethod): Check = {
    hive.sql(s"USE $database")
    sqlTable(hive, table, cacheMethod)
  }

}
