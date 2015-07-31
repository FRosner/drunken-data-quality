package de.frosner.ddq

import java.text.SimpleDateFormat

import de.frosner.ddq.Check._
import de.frosner.ddq.Constraint.ConstraintFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel

import scala.util.Try

case class Check(dataFrame: DataFrame,
                 cacheMethod: Option[StorageLevel] = Option(StorageLevel.MEMORY_ONLY),
                 constraints: Iterable[Constraint] = Iterable.empty) {
  
  private def addConstraint(cf: ConstraintFunction): Check = Check(dataFrame, cacheMethod, constraints ++ List(Constraint(cf)))
  
  def hasUniqueKey(columnName: String, columnNames: String*): Check = addConstraint {
    df => {
      val columnsString = (columnName :: columnNames.toList).mkString(",")
      val nonUniqueRows = df.groupBy(columnName, columnNames:_*).count.filter(new Column("count") > 1).count
      if (nonUniqueRows == 0)
        success(s"""Columns $columnsString are a key""")
      else
        failure(s"""Columns $columnsString are not a key""")
    }
  }
  
  def satisfies(constraint: String): Check = addConstraint {
    df => {
      val succeedingRows = df.filter(constraint).count
      val count = df.count
      if (succeedingRows == count)
        success(s"Constraint $constraint is satisfied")
      else
        failure(s"${count - succeedingRows} rows did not satisfy constraint $constraint")
    }
  }

  def isNeverNull(columnName: String) = addConstraint {
    df => {
      val nullCount = df.filter(new Column(columnName).isNull).count
      if (nullCount == 0)
        success(s"Column $columnName is not null")
      else
        failure(s"Column $columnName has $nullCount null rows although it should not be null")
    }
  }

  def isAlwaysNull(columnName: String) = addConstraint {
    df => {
      val notNullCount = df.filter(new Column(columnName).isNotNull).count
      if (notNullCount == 0)
        success(s"Column $columnName is null")
      else
        failure(s"Column $columnName has $notNullCount non-null rows although it should be null")
    }
  }
  
  def hasNumRowsEqualTo(expected: Long): Check = addConstraint {
    df => {
      val count = df.count
      if (count == expected)
        success(s"The number of rows is equal to $count")
      else
        failure(s"The actual number of rows $count is not equal to the expected $expected")
    }
  }

  private val cannotBeInt = udf((column: String) => column != null && Try(column.toInt).isFailure)
  def isConvertibleToInt(columnName: String) = addConstraint {
    df => {
      val cannotBeIntCount = df.filter(cannotBeInt(new Column(columnName))).count
      if (cannotBeIntCount == 0)
        success(s"Column $columnName can be converted to Int")
      else
        failure(s"Column $columnName contains $cannotBeIntCount rows that cannot be converted to Int")
    }
  }

  private val cannotBeDouble = udf((column: String) => column != null && Try(column.toDouble).isFailure)
  def isConvertibleToDouble(columnName: String) = addConstraint {
    df => {
      val cannotBeDoubleCount = df.filter(cannotBeDouble(new Column(columnName))).count
      if (cannotBeDoubleCount == 0)
        success(s"Column $columnName can be converted to Double")
      else
        failure(s"Column $columnName contains $cannotBeDoubleCount rows that cannot be converted to Double")
    }
  }

  private val cannotBeLong = udf((column: String) => column != null && Try(column.toLong).isFailure)
  def isConvertibleToLong(columnName: String) = addConstraint {
    df => {
      val cannotBeLongCount = df.filter(cannotBeLong(new Column(columnName))).count
      if (cannotBeLongCount == 0)
        success(s"Column $columnName can be converted to Long")
      else
        failure(s"Column $columnName contains $cannotBeLongCount rows that cannot be converted to Long")
    }
  }

  def isConvertibleToDate(columnName: String, dateFormat: SimpleDateFormat) = addConstraint {
    df => {
      val cannotBeDate = udf((column: String) => column != null && Try(dateFormat.parse(column)).isFailure)
      val cannotBeDateCount = df.filter(cannotBeDate(new Column(columnName))).count
      if (cannotBeDateCount == 0)
        success(s"Column $columnName can be converted to Date")
      else
        failure(s"Column $columnName contains $cannotBeDateCount rows that cannot be converted to Date")
    }
  }

  def isConvertibleToBoolean(columnName: String, trueValue: String = "true", falseValue: String = "false",
                              isCaseSensitive: Boolean = false) = addConstraint {
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
        success(s"Column $columnName can be converted to Boolean")
      else
        failure(s"Column $columnName contains $cannotBeBooleanCount rows that cannot be converted to Boolean")
    }
  }

  def hasForeignKey(referenceTable: DataFrame, keyMap: (String, String), keyMaps: (String, String)*) = addConstraint {
    df => {
      val columns = keyMap :: keyMaps.toList
      val renamedColumns = columns.map{ case (baseColumn, refColumn) => ("b_" + baseColumn, "r_" + refColumn)}
      val (baseColumns, refColumns) = columns.unzip
      val (renamedBaseColumns, renamedRefColumns) = renamedColumns.unzip

      // check if foreign key is a key in reference table
      val nonUniqueRows = referenceTable.groupBy(refColumns.map(new Column(_)):_*).count.filter(new Column("count") > 1).count
      if (nonUniqueRows > 0) {
        failure( s"""Columns ${refColumns.mkString(", ")} are not a key in reference table""")
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
          success(s"""Columns $columnsString define a foreign key""")
        else
          failure(s"Columns $columnsString do not define a foreign key ($notMatchingRefs records do not match)")
      }
    }
  }

  def run: Boolean = {
    hint(s"Checking $dataFrame")
    val potentiallyPersistedDf = cacheMethod.map(dataFrame.persist(_)).getOrElse(dataFrame)
    val result = if (!constraints.isEmpty)
      constraints.map(c => c.fun(potentiallyPersistedDf)).reduce(_ && _)
    else
      hint("- Nothing to check!")
    if (cacheMethod.isDefined) potentiallyPersistedDf.unpersist()
    result
  }
      
}

object Check {

  def success(message: String): Boolean = {
    println(Console.GREEN + "- " + message + Console.RESET)
    true
  }

  def failure(message: String): Boolean = {
    println(Console.RED + "- " + message + Console.RESET)
    false
  }

  def hint(message: String): Boolean = {
    println(Console.BLUE + message + Console.RESET)
    true
  }

}
