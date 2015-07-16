package de.frosner.ddq

import org.apache.spark.sql.{Column, DataFrame}
import Constraint.ConstraintFunction
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import Check._

import scala.util.Try

case class Check(dataFrame: DataFrame,
                 cacheMethod: Option[StorageLevel] = Option(StorageLevel.MEMORY_ONLY),
                 constraints: Iterable[Constraint] = Iterable.empty) {
  
  private def addConstraint(cf: ConstraintFunction): Check = Check(dataFrame, cacheMethod, constraints ++ List(Constraint(cf)))
  
  def hasKey(columnName: String, columnNames: String*): Check = addConstraint { 
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
