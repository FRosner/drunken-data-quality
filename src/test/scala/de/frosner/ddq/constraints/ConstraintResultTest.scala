package de.frosner.ddq.constraints

import java.text.SimpleDateFormat

import de.frosner.ddq.constraints.{ConstraintFailure, ConstraintSuccess}
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, Column}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}
import org.mockito.Mockito._

class ConstraintResultTest extends FlatSpec with Matchers with MockitoSugar {

  "An AlwaysNullConstraintResult" should "have the correct success message" in {
    val constraint = AlwaysNullConstraint("c")
    val result = AlwaysNullConstraintResult(constraint, 0L, ConstraintSuccess)
    result.message shouldBe "Column c is always null."
  }

  it should "have the correct failure message (one row)" in {
    val constraint = AlwaysNullConstraint("c")
    val result = AlwaysNullConstraintResult(constraint, 1L, ConstraintFailure)
    result.message shouldBe "Column c contains 1 non-null row (should always be null)."
  }

  it should "have the correct failure message (multiple rows)" in {
    val constraint = AlwaysNullConstraint("c")
    val result = AlwaysNullConstraintResult(constraint, 2L, ConstraintFailure)
    result.message shouldBe "Column c contains 2 non-null rows (should always be null)."
  }

  "An AnyOfConstraintResult" should "have the correct success message" in {
    val constraint = AnyOfConstraint("c", Set("a", "b"))
    val result = AnyOfConstraintResult(constraint, 0L, ConstraintSuccess)
    result.message shouldBe "Column c contains only values in Set(a, b)."
  }

  it should "have the correct failure message (one row)" in {
    val constraint = AnyOfConstraint("c", Set("a", "b"))
    val result = AnyOfConstraintResult(constraint, 1L, ConstraintFailure)
    result.message shouldBe "Column c contains 1 row that is not in Set(a, b)."
  }

  it should "have the correct failure message (multiple rows)" in {
    val constraint = AnyOfConstraint("c", Set("a", "b"))
    val result = AnyOfConstraintResult(constraint, 2L, ConstraintFailure)
    result.message shouldBe "Column c contains 2 rows that are not in Set(a, b)."
  }

  "A ColumnColumnConstraintResult" should "have the correct success message" in {
    val constraint = ColumnColumnConstraint(new Column("c") === 5)
    val result = ColumnColumnConstraintResult(constraint, 0L, ConstraintSuccess)
    result.message shouldBe "Constraint (c = 5) is satisfied."
  }

  it should "have the correct failure message (one row)" in {
    val constraint = ColumnColumnConstraint(new Column("c") === 5)
    val result = ColumnColumnConstraintResult(constraint, 1L, ConstraintFailure)
    result.message shouldBe "1 row did not satisfy constraint (c = 5)."
  }

  it should "have the correct failure message (multiple rows)" in {
    val constraint = ColumnColumnConstraint(new Column("c") === 5)
    val result = ColumnColumnConstraintResult(constraint, 2L, ConstraintFailure)
    result.message shouldBe "2 rows did not satisfy constraint (c = 5)."
  }

  "A ConditionalColumnConstraintResult" should "have the correct success message" in {
    val constraint = ConditionalColumnConstraint(new Column("c") === 5, new Column("d") === 2 )
    val result = ConditionalColumnConstraintResult(constraint, 0L, ConstraintSuccess)
    result.message shouldBe "Constraint (c = 5) -> (d = 2) is satisfied."
  }

  it should "have the correct failure message (one row)" in {
    val constraint = ConditionalColumnConstraint(new Column("c") === 5, new Column("d") === 2 )
    val result = ConditionalColumnConstraintResult(constraint, 1L, ConstraintFailure)
    result.message shouldBe "1 row did not satisfy constraint (c = 5) -> (d = 2)."
  }

  it should "have the correct failure message (multiple rows)" in {
    val constraint = ConditionalColumnConstraint(new Column("c") === 5, new Column("d") === 2 )
    val result = ConditionalColumnConstraintResult(constraint, 2L, ConstraintFailure)
    result.message shouldBe "2 rows did not satisfy constraint (c = 5) -> (d = 2)."
  }

  "A DateFormatConstraintResult" should "have the correct success message" in {
    val constraint = DateFormatConstraint("c", new SimpleDateFormat("yyyy"))
    val result = DateFormatConstraintResult(constraint, 0L, ConstraintSuccess)
    result.message shouldBe "Column c is formatted by yyyy."
  }

  it should "have the correct failure message (one row)" in {
    val constraint = DateFormatConstraint("c", new SimpleDateFormat("yyyy"))
    val result = DateFormatConstraintResult(constraint, 1L, ConstraintFailure)
    result.message shouldBe "Column c contains 1 row that is not formatted by yyyy."
  }

  it should "have the correct failure message (multiple rows)" in {
    val constraint = DateFormatConstraint("c", new SimpleDateFormat("yyyy"))
    val result = DateFormatConstraintResult(constraint, 2L, ConstraintFailure)
    result.message shouldBe "Column c contains 2 rows that are not formatted by yyyy."
  }

  "A ForeignKeyConstraintResult" should "have the correct success message (one column)" in {
    val ref = mock[DataFrame]
    when(ref.toString).thenReturn("ref")
    val constraint = ForeignKeyConstraint(Seq("a" -> "b"), ref)
    val result = ForeignKeyConstraintResult(constraint, Option(0L), ConstraintSuccess)
    result.message shouldBe "Column a->b defines a foreign key pointing to the reference table ref."
  }

  it should "have the correct success message (multiple columns)" in {
    val ref = mock[DataFrame]
    when(ref.toString).thenReturn("ref")
    val constraint = ForeignKeyConstraint(Seq("a" -> "b", "c" -> "d"), ref)
    val result = ForeignKeyConstraintResult(constraint, Option(0L), ConstraintSuccess)
    result.message shouldBe "Columns a->b, c->d define a foreign key pointing to the reference table ref."
  }

  it should "have the correct failure message if the columns are no key in the reference table (one column)" in {
    val ref = mock[DataFrame]
    when(ref.toString).thenReturn("ref")
    val constraint = ForeignKeyConstraint(Seq("a" -> "b"), ref)
    val result = ForeignKeyConstraintResult(constraint, None, ConstraintFailure)
    result.message shouldBe "Column a->b is not a key in the reference table."
  }

  it should "have the correct failure message if the columns are no key in the reference table (multiple columns)" in {
    val ref = mock[DataFrame]
    when(ref.toString).thenReturn("ref")
    val constraint = ForeignKeyConstraint(Seq("a" -> "b", "c" -> "d"), ref)
    val result = ForeignKeyConstraintResult(constraint, None, ConstraintFailure)
    result.message shouldBe "Columns a->b, c->d are not a key in the reference table."
  }

  it should "have the correct failure message if the columns don't define a foreign key (one column, one row)" in {
    val ref = mock[DataFrame]
    when(ref.toString).thenReturn("ref")
    val constraint = ForeignKeyConstraint(Seq("a" -> "b"), ref)
    val result = ForeignKeyConstraintResult(constraint, Some(1L), ConstraintFailure)
    result.message shouldBe "Column a->b does not define a foreign key pointing to ref. 1 row does not match."
  }

  it should "have the correct failure message if the columns don't define a foreign key (multiple columns, multiple rows)" in {
    val ref = mock[DataFrame]
    when(ref.toString).thenReturn("ref")
    val constraint = ForeignKeyConstraint(Seq("a" -> "b", "c" -> "d"), ref)
    val result = ForeignKeyConstraintResult(constraint, Some(2L), ConstraintFailure)
    result.message shouldBe "Columns a->b, c->d do not define a foreign key pointing to ref. 2 rows do not match."
  }

  "A FunctionalDependencyConstraintResult" should "have the correct success message (one/one column)" in {
    val constraint = FunctionalDependencyConstraint(Seq("a"), Seq("c"))
    val result = FunctionalDependencyConstraintResult(constraint, 0L, ConstraintSuccess)
    result.message shouldBe "Column c is functionally dependent on a."
  }

  it should "have the correct success message (one/multiple columns)" in {
    val constraint = FunctionalDependencyConstraint(Seq("a"), Seq("c", "d"))
    val result = FunctionalDependencyConstraintResult(constraint, 0L, ConstraintSuccess)
    result.message shouldBe "Columns c, d are functionally dependent on a."
  }

  it should "have the correct success message (multiple/one columns)" in {
    val constraint = FunctionalDependencyConstraint(Seq("a", "b"), Seq("c"))
    val result = FunctionalDependencyConstraintResult(constraint, 0L, ConstraintSuccess)
    result.message shouldBe "Column c is functionally dependent on a, b."
  }

  it should "have the correct success message (multiple/multiple columns)" in {
    val constraint = FunctionalDependencyConstraint(Seq("a", "b"), Seq("c", "d"))
    val result = FunctionalDependencyConstraintResult(constraint, 0L, ConstraintSuccess)
    result.message shouldBe "Columns c, d are functionally dependent on a, b."
  }

  it should "have the correct failure message (one/one column, one row)" in {
    val constraint = FunctionalDependencyConstraint(Seq("a"), Seq("c"))
    val result = FunctionalDependencyConstraintResult(constraint, 1L, ConstraintFailure)
    result.message shouldBe "Column c is not functionally dependent on a (1 violating determinant value)."
  }

  it should "have the correct failure message (one/multiple columns, multiple rows)" in {
    val constraint = FunctionalDependencyConstraint(Seq("a"), Seq("c", "d"))
    val result = FunctionalDependencyConstraintResult(constraint, 2L, ConstraintFailure)
    result.message shouldBe "Columns c, d are not functionally dependent on a (2 violating determinant values)."
  }

  it should "have the correct failure message (multiple/one columns, one row)" in {
    val constraint = FunctionalDependencyConstraint(Seq("a", "b"), Seq("c"))
    val result = FunctionalDependencyConstraintResult(constraint, 1L, ConstraintFailure)
    result.message shouldBe "Column c is not functionally dependent on a, b (1 violating determinant value)."
  }

  it should "have the correct failure message (multiple/multiple columns, multiple rows)" in {
    val constraint = FunctionalDependencyConstraint(Seq("a", "b"), Seq("c", "d"))
    val result = FunctionalDependencyConstraintResult(constraint, 5L, ConstraintFailure)
    result.message shouldBe "Columns c, d are not functionally dependent on a, b (5 violating determinant values)."
  }

  "A JoinableConstraintResult" should "have the correct success message" in {
    val ref = mock[DataFrame]
    when(ref.toString).thenReturn("ref")
    val constraint = JoinableConstraint(Seq("c1" -> "c2"), ref)
    val result = JoinableConstraintResult(constraint, 1L, 1L, ConstraintSuccess)
    result.message shouldBe "Key c1->c2 can be used for joining. " +
      "Join columns cardinality in base table: 1. " +
      "Join columns cardinality after joining: 1 (100.00%)."
  }

  it should "compute the correct match percentage in the success message" in {
    val ref = mock[DataFrame]
    when(ref.toString).thenReturn("ref")
    val constraint = JoinableConstraint(Seq("c1" -> "c2"), ref)
    val result = JoinableConstraintResult(constraint, 2L, 1L, ConstraintSuccess)
    result.message shouldBe "Key c1->c2 can be used for joining. " +
      "Join columns cardinality in base table: 2. " +
      "Join columns cardinality after joining: 1 (50.00%)."
  }

  it should "have the correct failure message" in {
    val ref = mock[DataFrame]
    when(ref.toString).thenReturn("ref")
    val constraint = JoinableConstraint(Seq("c1" -> "c2", "c5" -> "c2"), ref)
    val result = JoinableConstraintResult(constraint, 5L, 0L, ConstraintFailure)
    result.message shouldBe "Key c1->c2, c5->c2 cannot be used for joining (no result)."
  }

  "A NeverNullConstraintResult" should "have the correct success message" in {
    val constraint = NeverNullConstraint("c")
    val result = NeverNullConstraintResult(constraint, 0L, ConstraintSuccess)
    result.message shouldBe "Column c is never null."
  }

  it should "have the correct failure message (one row)" in {
    val constraint = NeverNullConstraint("c")
    val result = NeverNullConstraintResult(constraint, 1L, ConstraintFailure)
    result.message shouldBe "Column c contains 1 row that is null (should never be null)."
  }

  it should "have the correct failure message (multiple rows)" in {
    val constraint = NeverNullConstraint("c")
    val result = NeverNullConstraintResult(constraint, 2L, ConstraintFailure)
    result.message shouldBe "Column c contains 2 rows that are null (should never be null)."
  }

  "A NumberOfRowsConstraintResult" should "have the correct success message" in {
    val constraint = NumberOfRowsConstraint(5L)
    val result = NumberOfRowsConstraintResult(constraint, 5L, ConstraintSuccess)
    result.message shouldBe "The number of rows is equal to 5."
  }

  it should "have the correct failure message" in {
    val constraint = NumberOfRowsConstraint(5L)
    val result = NumberOfRowsConstraintResult(constraint, 4L, ConstraintFailure)
    result.message shouldBe "The actual number of rows 4 is not equal to the expected 5."
  }

  "A RegexConstraintResult" should "have the correct success message" in {
    val constraint = RegexConstraint("c", ".*")
    val result = RegexConstraintResult(constraint, 0L, ConstraintSuccess)
    result.message shouldBe "Column c matches .*"
  }

  it should "have the correct failure message (one row)" in {
    val constraint = RegexConstraint("c", ".*")
    val result = RegexConstraintResult(constraint, 1L, ConstraintFailure)
    result.message shouldBe "Column c contains 1 row that does not match .*"
  }

  it should "have the correct failure message (two rows)" in {
    val constraint = RegexConstraint("c", ".*")
    val result = RegexConstraintResult(constraint, 2L, ConstraintFailure)
    result.message shouldBe "Column c contains 2 rows that do not match .*"
  }

  "A StringColumnConstraintResult" should "have the correct success message" in {
    val constraint = StringColumnConstraint("column > 0")
    val result = StringColumnConstraintResult(constraint, 0L, ConstraintSuccess)
    result.message shouldBe "Constraint column > 0 is satisfied."
  }

  it should "have the correct failure message (one row)" in {
    val constraint = StringColumnConstraint("column > 0")
    val result = StringColumnConstraintResult(constraint, 1L, ConstraintFailure)
    result.message shouldBe "1 row did not satisfy constraint column > 0."
  }

  it should "have the correct failure message (multiple rows)" in {
    val constraint = StringColumnConstraint("column > 0")
    val result = StringColumnConstraintResult(constraint, 2L, ConstraintFailure)
    result.message shouldBe "2 rows did not satisfy constraint column > 0."
  }

  "A TypeConversionConstraintResult" should "have the correct success message" in {
    val constraint = TypeConversionConstraint("c", IntegerType)
    val result = TypeConversionConstraintResult(constraint, StringType, 0L, ConstraintSuccess)
    result.message shouldBe "Column c can be converted from StringType to IntegerType."
  }

  it should "have the correct failure message (one row)" in {
    val constraint = TypeConversionConstraint("c", IntegerType)
    val result = TypeConversionConstraintResult(constraint, StringType, 1L, ConstraintFailure)
    result.message shouldBe "Column c cannot be converted from StringType to IntegerType. 1 row could not be converted."
  }

  it should "have the correct failure message (multiple rows)" in {
    val constraint = TypeConversionConstraint("c", IntegerType)
    val result = TypeConversionConstraintResult(constraint, StringType, 2L, ConstraintFailure)
    result.message shouldBe "Column c cannot be converted from StringType to IntegerType. 2 rows could not be converted."
  }

  "A UniqueKeyConstraintResult" should "have the correct success message (single column)" in {
    val constraint = UniqueKeyConstraint(Seq("column1"))
    val result = UniqueKeyConstraintResult(constraint, 0L, ConstraintSuccess)
    result.message shouldBe "Column column1 is a key."
  }

  it should "have the correct success message (multiple columns)" in {
    val constraint = UniqueKeyConstraint(Seq("column1", "column2"))
    val result = UniqueKeyConstraintResult(constraint, 0L, ConstraintSuccess)
    result.message shouldBe "Columns column1, column2 are a key."
  }

  it should "have the correct failure message (one column, one row)" in {
    val constraint = UniqueKeyConstraint(Seq("column1"))
    val result = UniqueKeyConstraintResult(constraint, 1L, ConstraintFailure)
    result.message shouldBe "Column column1 is not a key (1 non-unique tuple)."
  }

  it should "have the correct failure message (multiple columns, multiple rows)" in {
    val constraint = UniqueKeyConstraint(Seq("column1", "column2"))
    val result = UniqueKeyConstraintResult(constraint, 2L, ConstraintFailure)
    result.message shouldBe "Columns column1, column2 are not a key (2 non-unique tuples)."
  }

}
