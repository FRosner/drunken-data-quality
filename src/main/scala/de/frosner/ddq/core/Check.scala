package de.frosner.ddq.core

import java.text.SimpleDateFormat
import java.util.regex.Pattern

import de.frosner.ddq.core
import de.frosner.ddq.reporters.{ConsoleReporter, Reporter}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
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
                 cacheMethod: Option[StorageLevel] = Check.DEFAULT_CACHE_METHOD,
                 constraints: Seq[Constraint] = Seq.empty) {

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
  def isNeverNull(columnName: String) = addConstraint(Check.isNeverNull(columnName))

  /**
   * Check whether the column with the given name contains only null values.
   *
   * @param columnName Name of the column to check
   * @return [[core.Check]] object including this constraint
   */
  def isAlwaysNull(columnName: String) = addConstraint(Check.isAlwaysNull(columnName))

  /**
   * Check whether the table has exactly the given number of rows.
   *
   * @param expected Expected number of rows.
   * @return [[core.Check]] object including this constraint
   */
  def hasNumRowsEqualTo(expected: Long): Check = addConstraint(Check.hasNumRowsEqualTo(expected))

  /**
   * Check whether the column with the given name can be converted to an integer.
   *
   * @param columnName Name of the column to check
   * @return [[core.Check]] object including this constraint
   */
  def isConvertibleToInt(columnName: String) = addConstraint(Check.isConvertibleToInt(columnName))

  /**
   * Check whether the column with the given name can be converted to a double.
   *
   * @param columnName Name of the column to check
   * @return [[core.Check]] object including this constraint
   */
  def isConvertibleToDouble(columnName: String) = addConstraint(Check.isConvertibleToDouble(columnName))

  /**
   * Check whether the column with the given name can be converted to a long.
   *
   * @param columnName Name of the column to check
   * @return [[core.Check]] object including this constraint
   */
  def isConvertibleToLong(columnName: String) = addConstraint(Check.isConvertibleToLong(columnName))

  /**
   * Check whether the column with the given name can be converted to a date using the specified date format.
   *
   * @param columnName Name of the column to check
   * @param dateFormat Date format to use for conversion
   * @return [[core.Check]] object including this constraint
   */
  def isConvertibleToDate(columnName: String, dateFormat: SimpleDateFormat) = addConstraint(
    Check.isConvertibleToDate(columnName, dateFormat))

  /**
   * Check whether the column with the given name is always any of the specified values.
   *
   * @param columnName Name of the column to check
   * @param allowed Set of allowed values
   * @return [[core.Check]] object including this constraint
   */
  def isAnyOf(columnName: String, allowed: Set[Any]) = addConstraint(Check.isAnyOf(columnName, allowed))

  /**
   * Check whether the column with the given name is always matching the specified regular expression.
   *
   * @param columnName Name of the column to check
   * @param regex Regular expression that needs to match
   * @return [[core.Check]] object including this constraint
   */
  def isMatchingRegex(columnName: String, regex: String) = addConstraint(Check.isMatchingRegex(columnName, regex))

  /**
   * Check whether the column with the given name can be converted to a boolean. You can specify the textual values
   * to use for true and false.
   *
   * @param columnName Name of the column to check
   * @param trueValue String value to treat as true
   * @param falseValue String value to treat as false
   * @param isCaseSensitive Whether parsing should be case sensitive
   * @return [[core.Check]] object including this constraint
   */
  def isConvertibleToBoolean(columnName: String, trueValue: String = "true", falseValue: String = "false",
                             isCaseSensitive: Boolean = false) =
  addConstraint(Check.isConvertibleToBoolean(columnName, trueValue, falseValue, isCaseSensitive))

  /**
   * Check whether the columns with the given names define a foreign key to the specified reference table.
   *
   * @param referenceTable Table to which the foreign key is pointing
   * @param keyMap Column mapping from this table to the reference one (`"column1" -> "base_column1"`)
   * @param keyMaps Column mappings from this table to the reference one (`"column1" -> "base_column1"`)
   * @return [[core.Check]] object including this constraint
   */
  def hasForeignKey(referenceTable: DataFrame, keyMap: (String, String), keyMaps: (String, String)*) = addConstraint(
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
  def isJoinableWith(referenceTable: DataFrame, keyMap: (String, String), keyMaps: (String, String)*) = addConstraint(
    Check.isJoinableWith(referenceTable, keyMap, keyMaps: _*)
  )

  /**
   * Check whether the columns in the dependent set have a functional dependency on determinant set.
   *
   * @param determinantSet sequence of column names which form a determinant set
   * @param dependentSet sequence of column names which form a dependent set
   * @return [[core.Check]] object including this constraint
   */
  def hasFunctionalDependency(determinantSet: Seq[String], dependentSet: Seq[String]) = addConstraint(
    Check.hasFunctionalDependency(determinantSet, dependentSet)
  )

  /**
   * Run check with all the previously specified constraints and report to every reporter passed as an argument
   *
   * @param reporters iterable of reporters to produce output on the check result
   * @return check result
   **/
  def run(reporters: Reporter*): CheckResult = {
    val actualReporters = if (reporters.isEmpty) List(ConsoleReporter(System.out)) else reporters
    Runner.run(List(this), actualReporters)(this)
  }

}

object Check {

  private val DEFAULT_CACHE_METHOD = Option(StorageLevel.MEMORY_ONLY)

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
               cacheMethod: Option[StorageLevel] = DEFAULT_CACHE_METHOD): Check = {
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
   * @return [[core.Constraint]] object
   */
  def hasUniqueKey(columnName: String, columnNames: String*): Constraint = Constraint(
    df => {
      val columnsList = columnName :: columnNames.toList
      val columnsString = columnsList.mkString(",")
      val nonUniqueRows = df.groupBy(columnName, columnNames:_*).count.filter(new Column("count") > 1).count
      if (nonUniqueRows == 0)
        ConstraintSuccess(s"""${if(columnsList.length == 1) s"Column $columnsString is a key" else s"Columns $columnsString are a key"}""")
      else
        ConstraintFailure(s"""${if(columnsList.length == 1) s"Column $columnsString is not a key" else s"Columns $columnsString are not a key"}""")
    }
  )

  /**
   * Check whether the table has exactly the given number of rows.
   *
   * @param expected Expected number of rows.
   * @return [[core.Constraint]] object
   */
  def hasNumRowsEqualTo(expected: Long): Constraint = Constraint(
    df => {
      val count = df.count
      if (count == expected)
        ConstraintSuccess(s"The number of rows is equal to $count")
      else
        ConstraintFailure(s"The actual number of rows $count is not equal to the expected $expected")
    }
  )

  private def satisfies(succeedingRowsFunction: DataFrame => DataFrame, constraintString: String): Constraint = Constraint(
    df => {
      val succeedingRows = succeedingRowsFunction(df).count
      val count = df.count
      val failingRows = count - succeedingRows
      if (succeedingRows == count)
        ConstraintSuccess(s"Constraint $constraintString is satisfied")
      else
        ConstraintFailure(s"""${if(failingRows == 1) "One row" else s"$failingRows rows"} did not satisfy constraint $constraintString""")
    }
  )

  /**
   * Check whether the given constraint is satisfied. The constraint has to comply with Spark SQL syntax. So you
   * can just write it the same way that you would put it inside a `WHERE` clause.
   *
   * @param constraint The constraint that needs to be satisfied for all columns
   * @return [[core.Constraint]] object
   */
  def satisfies(constraint: String): Constraint = Check.satisfies(
    (df: DataFrame) => df.filter(constraint),
    constraint
  )

  /**
   * Check whether the given constraint is satisfied. The constraint is built using the
   * [[org.apache.spark.sql.Column]] class.
   *
   * @param constraint The constraint that needs to be satisfied for all columns
   * @return [[core.Constraint]] object
   */
  def satisfies(constraint: Column): Constraint = Check.satisfies(
    (df: DataFrame) => df.filter(constraint),
    constraint.toString()
  )

  /**
   * <p>Check whether the given conditional constraint is satisfied. The constraint is built using the
   * [[org.apache.spark.sql.Column]] class.</p><br/>
   * Usage:
   * {{{
   * Check(df).satisfies((new Column("c1") === 1) -> (new Column("c2").isNotNull))
   * }}}
   *
   * @param conditional The constraint that needs to be satisfied for all columns
   * @return [[core.Constraint]] object
   */
  def satisfies(conditional: (Column, Column)): Constraint = {
    val (statement, implication) = conditional
    Check.satisfies(
      (df: DataFrame) => df.filter(!statement || implication),
      s"$statement -> $implication"
    )
  }

  /**
   * Check whether the column with the given name contains only null values.
   *
   * @param columnName Name of the column to check
   * @return [[core.Constraint]] object
   */
  def isAlwaysNull(columnName: String) = Constraint(
    df => {
      val notNullCount = df.filter(new Column(columnName).isNotNull).count
      if (notNullCount == 0)
        ConstraintSuccess(s"Column $columnName is null")
      else
        ConstraintFailure(s"Column $columnName has ${if(notNullCount == 1) "one non-null row" else s"$notNullCount non-null rows"} although it should be null")
    }
  )

  /**
   * Check whether the column with the given name contains no null values.
   *
   * @param columnName Name of the column to check
   * @return [[core.Constraint]] object
   */
  def isNeverNull(columnName: String) = Constraint(
    df => {
      val nullCount = df.filter(new Column(columnName).isNull).count
      if (nullCount == 0)
        ConstraintSuccess(s"Column $columnName is not null")
      else
        ConstraintFailure(s"Column $columnName has ${if(nullCount == 1) "one null row" else s"$nullCount null rows"} although it should not be null")
    }
  )

  private val cannotBeInt = udf((column: String) => column != null && Try(column.toInt).isFailure)
  /**
   * Check whether the column with the given name can be converted to an integer.
   *
   * @param columnName Name of the column to check
   * @return [[core.Constraint]] object
   */
  def isConvertibleToInt(columnName: String) = Constraint(
    df => {
      val cannotBeIntCount = df.filter(cannotBeInt(new Column(columnName))).count
      if (cannotBeIntCount == 0)
        ConstraintSuccess(s"Column $columnName can be converted to Int")
      else
        ConstraintFailure(s"Column $columnName contains ${if(cannotBeIntCount == 1) "one row" else s"$cannotBeIntCount rows"} that cannot be converted to Int")
    }
  )

  private val cannotBeDouble = udf((column: String) => column != null && Try(column.toDouble).isFailure)
  /**
   * Check whether the column with the given name can be converted to a double.
   *
   * @param columnName Name of the column to check
   * @return [[core.Constraint]] object
   */
  def isConvertibleToDouble(columnName: String) = Constraint(
    df => {
      val cannotBeDoubleCount = df.filter(cannotBeDouble(new Column(columnName))).count
      if (cannotBeDoubleCount == 0)
        ConstraintSuccess(s"Column $columnName can be converted to Double")
      else
        ConstraintFailure(s"Column $columnName contains ${if(cannotBeDoubleCount == 1) "one row" else s"$cannotBeDoubleCount rows"} that cannot be converted to Double")
    }
  )

  private val cannotBeLong = udf((column: String) => column != null && Try(column.toLong).isFailure)
  /**
   * Check whether the column with the given name can be converted to a long.
   *
   * @param columnName Name of the column to check
   * @return [[core.Constraint]] object
   */
  def isConvertibleToLong(columnName: String) = Constraint(
    df => {
      val cannotBeLongCount = df.filter(cannotBeLong(new Column(columnName))).count
      if (cannotBeLongCount == 0)
        ConstraintSuccess(s"Column $columnName can be converted to Long")
      else
        ConstraintFailure(s"Column $columnName contains ${if(cannotBeLongCount == 1) "one row" else s"$cannotBeLongCount rows"} that cannot be converted to Long")
    }
  )

  /**
   * Check whether the column with the given name can be converted to a date using the specified date format.
   *
   * @param columnName Name of the column to check
   * @param dateFormat Date format to use for conversion
   * @return [[core.Constraint]] object
   */
  def isConvertibleToDate(columnName: String, dateFormat: SimpleDateFormat) = Constraint(
    df => {
      val cannotBeDate = udf((column: String) => column != null && Try(dateFormat.parse(column)).isFailure)
      val cannotBeDateCount = df.filter(cannotBeDate(new Column(columnName))).count
      if (cannotBeDateCount == 0)
        ConstraintSuccess(s"Column $columnName can be converted to Date")
      else
        ConstraintFailure(s"Column $columnName contains ${if(cannotBeDateCount == 1) "one row" else s"$cannotBeDateCount rows"} that cannot be converted to Date")
    }
  )

  /**
   * Check whether the column with the given name is always any of the specified values.
   *
   * @param columnName Name of the column to check
   * @param allowed Set of allowed values
   * @return [[core.Constraint]] object
   */
  def isAnyOf(columnName: String, allowed: Set[Any]) = Constraint(
    df => {
      df.select(new Column(columnName)) // check if reference is not ambiguous
      val columnIndex = df.columns.indexOf(columnName)
      val notAllowedCount = df.rdd.filter(row => !row.isNullAt(columnIndex) && !allowed.contains(row.get(columnIndex))).count
      if (notAllowedCount == 0)
        ConstraintSuccess(s"Column $columnName contains only values in $allowed")
      else
        ConstraintFailure(s"Column $columnName contains ${if(notAllowedCount == 1) "one row" else s"$notAllowedCount rows"} that are not in $allowed")
    }
  )

  /**
   * Check whether the column with the given name is always matching the specified regular expression.
   *
   * @param columnName Name of the column to check
   * @param regex Regular expression that needs to match
   * @return [[core.Constraint]] object
   */
  def isMatchingRegex(columnName: String, regex: String) = Constraint(
    df => {
      val pattern = Pattern.compile(regex)
      val doesNotMatch = udf((column: String) => column != null && !pattern.matcher(column).find())
      val doesNotMatchCount = df.filter(doesNotMatch(new Column(columnName))).count
      if (doesNotMatchCount == 0)
        ConstraintSuccess(s"Column $columnName matches $regex")
      else
        ConstraintFailure(s"Column $columnName contains ${if(doesNotMatchCount == 1) "one row that does not" else s"$doesNotMatchCount rows that do not"} match $regex")
    }
  )

  /**
   * Check whether the column with the given name can be converted to a boolean. You can specify the textual values
   * to use for true and false.
   *
   * @param columnName Name of the column to check
   * @param trueValue String value to treat as true
   * @param falseValue String value to treat as false
   * @param isCaseSensitive Whether parsing should be case sensitive
   * @return [[core.Constraint]] object
   */
  def isConvertibleToBoolean(columnName: String, trueValue: String = "true", falseValue: String = "false",
                             isCaseSensitive: Boolean = false) = Constraint(
    df => {
      val cannotBeBoolean =
        if (isCaseSensitive)
          udf((column: String) => column != null
            && column != trueValue
            && column != falseValue)
        else
          udf((column: String) => column != null
            && column.toUpperCase != trueValue.toUpperCase
            && column.toUpperCase != falseValue.toUpperCase)
      val cannotBeBooleanCount = df.filter(cannotBeBoolean(new Column(columnName))).count
      if (cannotBeBooleanCount == 0)
        ConstraintSuccess(s"Column $columnName can be converted to Boolean")
      else
        ConstraintFailure(s"Column $columnName contains ${if(cannotBeBooleanCount == 1) "one row" else s"$cannotBeBooleanCount rows"} that cannot be converted to Boolean")
    }
  )

  /**
   * Check whether the columns with the given names define a foreign key to the specified reference table.
   *
   * @param referenceTable Table to which the foreign key is pointing
   * @param keyMap Column mapping from this table to the reference one (`"column1" -> "base_column1"`)
   * @param keyMaps Column mappings from this table to the reference one (`"column1" -> "base_column1"`)
   * @return [[core.Constraint]] object
   */
  def hasForeignKey(referenceTable: DataFrame, keyMap: (String, String), keyMaps: (String, String)*) = Constraint(
    df => {
      val columns = keyMap :: keyMaps.toList
      val renamedColumns = columns.map{ case (baseColumn, refColumn) => ("b_" + baseColumn, "r_" + refColumn)}
      val (baseColumns, refColumns) = columns.unzip
      val (renamedBaseColumns, renamedRefColumns) = renamedColumns.unzip

      // check if foreign key is a key in reference table
      val nonUniqueRows = referenceTable.groupBy(refColumns.map(new Column(_)):_*).count.filter(new Column("count") > 1).count
      if (nonUniqueRows > 0) {
        ConstraintFailure( s"""Columns ${refColumns.mkString(", ")} are not a key in reference table""")
      } else {
        // rename all columns to avoid ambiguous column references
        val renamedDf = df.select(baseColumns.zip(renamedBaseColumns).map {
          case (original, renamed) => new Column(original).as(renamed)
        }:_*)
        val renamedRef = referenceTable.select(refColumns.zip(renamedRefColumns).map {
          case (original, renamed) => new Column(original).as(renamed)
        }:_*)

        // check if left outer join yields some null values
        val leftOuterJoin = renamedDf.distinct.join(renamedRef, renamedColumns.map{
          case (baseColumn, refColumn) => new Column(baseColumn) === new Column(refColumn)
        }.reduce(_ && _), "outer")
        val notMatchingRefs = leftOuterJoin.filter(renamedRefColumns.map(new Column(_).isNull).reduce(_ && _)).count
        val columnsString = columns.map{ case (baseCol, refCol) => baseCol + "->" + refCol }.mkString(", ")
        if (notMatchingRefs == 0)
          ConstraintSuccess(s"${if(columns.length == 1) s"Column $columnsString defines a foreign key" else s"Columns $columnsString define a foreign key"}")
        else
          ConstraintFailure(s"${if(columns.length == 1) s"Column $columnsString does not define a foreign key" else s"Columns $columnsString do not define a foreign key"} (${ if(notMatchingRefs == 1) "one record does not match" else s"$notMatchingRefs records do not match"})")
      }
    }
  )

  /**
   * Check whether a join between this table and the given reference table returns any results. This can be seen
   * as a weaker version of the foreign key check, as it requires only partial matches.
   *
   * @param referenceTable Table to join with
   * @param keyMap Column mapping from this table to the reference one (`"column1" -> "base_column1"`)
   * @param keyMaps Column mappings from this table to the reference one (`"column1" -> "base_column1"`)
   * @return [[core.Constraint]] object
   */
  def isJoinableWith(referenceTable: DataFrame, keyMap: (String, String), keyMaps: (String, String)*) = Constraint(
    df => {
      val columns = keyMap :: keyMaps.toList
      val columnsMap = columns.toMap
      val renamedColumns = columns.map{ case (baseColumn, refColumn) => ("b_" + baseColumn, "r_" + refColumn)}
      val (baseColumns, refColumns) = columns.unzip
      val (renamedBaseColumns, renamedRefColumns) = renamedColumns.unzip

      val nonUniqueRows = referenceTable.groupBy(refColumns.map(new Column(_)):_*).count.filter(new Column("count") > 1).count

      // rename all columns to avoid ambiguous column references
      val renamedDf = df.select(baseColumns.zip(renamedBaseColumns).map {
        case (original, renamed) => new Column(original).as(renamed)
      }:_*)
      val renamedRef = referenceTable.select(refColumns.zip(renamedRefColumns).map {
        case (original, renamed) => new Column(original).as(renamed)
      }:_*)

      // check if join yields some values
      val renamedDfDistinct = renamedDf.distinct
      val distinctBefore = renamedDfDistinct.count
      val join = renamedDfDistinct.join(renamedRef, renamedColumns.map{
        case (baseColumn, refColumn) => new Column(baseColumn) === new Column(refColumn)
      }.reduce(_ && _))
      val matchingRows = join.distinct.count
      val unmatchedKeysPercentage = ((matchingRows.toDouble / distinctBefore) * 100).round

      val columnNoun = if(columns.length == 1) "Column" else "Columns"
      val columnsString = columns.map{ case (baseCol, refCol) => baseCol + "->" + refCol }.mkString(", ")
      if (matchingRows > 0)
        ConstraintSuccess(f"""$columnNoun $columnsString can be used for joining (number of distinct rows in base table: $distinctBefore, number of distinct rows after joining: $matchingRows, unmatched keys in base table: $unmatchedKeysPercentage""" + "%)")
      else
        ConstraintFailure(s"$columnNoun $columnsString cannot be used for joining (no rows match)")
    }
  )

  /**
   * Check whether the columns in the dependent set have a functional dependency on determinant set.
   *
   * @param determinantSet sequence of column names which form a determinant set
   * @param dependentSet sequence of column names which form a dependent set
   * @return [[core.Constraint]] object
   */
  def hasFunctionalDependency(determinantSet: Seq[String], dependentSet: Seq[String]) = {
    require(determinantSet.nonEmpty, "determinantSet must not be empty")
    require(dependentSet.nonEmpty, "dependentSet must not be empty")
    Constraint(
      df => {
        val determinantColumns = determinantSet.map(columnName => new Column(columnName))
        val dependentColumns = dependentSet.map(columnName => new Column(columnName))
        val relevantSelection = df.select((determinantColumns ++ dependentColumns): _*)

        val determinantValueCounts = relevantSelection.distinct.groupBy(determinantColumns: _*).count
        val violatingDeterminantValuesCount = determinantValueCounts.filter(new Column("count") !== 1).count

        val determinantString = s"[${determinantSet.mkString(", ")}]"
        val dependentString = s"[${dependentSet.mkString(", ")}]"

        if (violatingDeterminantValuesCount == 0)
          ConstraintSuccess(s"Columns $dependentString are functionally dependent on $determinantString")
        else
          ConstraintFailure(s"Columns $dependentString are not functionally dependent on " +
            s"$determinantString ($violatingDeterminantValuesCount violating determinant values)")
      }
    )
  }


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
                cacheMethod: Option[StorageLevel] = DEFAULT_CACHE_METHOD): Check = {
    hive.sql(s"USE $database")
    sqlTable(hive, table, cacheMethod)
  }

}
