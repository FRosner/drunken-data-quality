import unittest

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import types as t

from pyddq.core import Check
from pyddq.reporters import MarkdownReporter
from pyddq.streams import ByteArrayOutputStream


class ConstraintTest(unittest.TestCase):
    def setUp(self):
        self.sc = SparkContext()
        self.sqlContext = SQLContext(self.sc)
        self.reporter = MarkdownReporter(ByteArrayOutputStream())

    def tearDown(self):
        self.sc.stop()

    def test_hasUniqueKey(self):
        df = self.sqlContext.createDataFrame([(1, "a"), (1, None), (3, "c")])
        check = Check(df).hasUniqueKey("_1").hasUniqueKey("_1", "_2")
        check.run([self.reporter])
        expected_output = """
**Checking [_1: bigint, _2: string]**

It has a total number of 2 columns and 3 rows.

- *FAILURE*: Column _1 is not a key (1 non-unique tuple).
- *SUCCESS*: Columns _1, _2 are a key.
""".strip()
        self.assertEqual(
            self.reporter.output_stream.get_output(),
            expected_output
        )

    def test_isNeverNull(self):
        df = self.sqlContext.createDataFrame([(1, "a"), (1, None), (3, "c")])
        check = Check(df).isNeverNull("_1").isNeverNull("_2")
        check.run([self.reporter])
        expected_output = """
**Checking [_1: bigint, _2: string]**

It has a total number of 2 columns and 3 rows.

- *SUCCESS*: Column _1 is never null.
- *FAILURE*: Column _2 contains 1 row that is null (should never be null).
""".strip()
        self.assertEqual(
            self.reporter.output_stream.get_output(),
            expected_output
        )

    def test_isAlwaysNull(self):
        schema = t.StructType([
            t.StructField("_1", t.IntegerType()),
            t.StructField("_2", t.StringType()),
        ])
        df = self.sqlContext.createDataFrame(
            [(1, None), (1, None), (3, None)],
            schema
        )
        check = Check(df).isAlwaysNull("_1").isAlwaysNull("_2")
        check.run([self.reporter])
        expected_output = """
**Checking [_1: int, _2: string]**

It has a total number of 2 columns and 3 rows.

- *FAILURE*: Column _1 contains 3 non-null rows (should always be null).
- *SUCCESS*: Column _2 is always null.
""".strip()
        self.assertEqual(
            self.reporter.output_stream.get_output(),
            expected_output
        )

    def test_isConvertibleTo(self):
        df = self.sqlContext.createDataFrame([(1, "a"), (1, None), (3, "c")])
        check = Check(df)\
                .isConvertibleTo("_1", t.IntegerType())\
                .isConvertibleTo("_1", t.ArrayType(t.IntegerType()))
        check.run([self.reporter])
        expected_output = """
**Checking [_1: bigint, _2: string]**

It has a total number of 2 columns and 3 rows.

- *SUCCESS*: Column _1 can be converted from LongType to IntegerType.
- *ERROR*: Checking whether column _1 can be converted to ArrayType(IntegerType,true) failed: org.apache.spark.sql.AnalysisException: cannot resolve 'cast(_1 as array<int>)' due to data type mismatch: cannot cast LongType to ArrayType(IntegerType,true);
""".strip()
        self.assertEqual(
            self.reporter.output_stream.get_output(),
            expected_output
        )

    def test_isFormattedAsDate(self):
        df = self.sqlContext.createDataFrame([
            ("2000-11-23 11:50:10", ),
            ("2000-5-23 11:50:10", ),
            ("2000-02-23 11:11:11", )
        ])
        check = Check(df).isFormattedAsDate("_1", "yyyy-MM-dd HH:mm:ss")
        check.run([self.reporter])
        expected_output = """
**Checking [_1: string]**

It has a total number of 1 columns and 3 rows.

- *SUCCESS*: Column _1 is formatted by yyyy-MM-dd HH:mm:ss.
""".strip()
        self.assertEqual(
            self.reporter.output_stream.get_output(),
            expected_output
        )

    def test_isAnyOf(self):
        df = self.sqlContext.createDataFrame([(1, "a"), (2, "b"), (3, "c")])
        check = Check(df).isAnyOf("_1", [1, 2]).isAnyOf("_2", ["a", "b", "c"])
        check.run([self.reporter])
        expected_output = """
**Checking [_1: bigint, _2: string]**

It has a total number of 2 columns and 3 rows.

- *FAILURE*: Column _1 contains 1 row that is not in Set(1, 2).
- *SUCCESS*: Column _2 contains only values in Set(a, b, c).
""".strip()
        self.assertEqual(
            self.reporter.output_stream.get_output(),
            expected_output
        )

    def test_isMatchingRegex(self):
        df = self.sqlContext.createDataFrame([
            ("Hello A", "world"),
            ("Hello B", None),
            ("Hello C", "World")
        ])
        check = Check(df)\
                .isMatchingRegex("_1", "^Hello")\
                .isMatchingRegex("_2", "world$")

        check.run([self.reporter])
        expected_output = """
**Checking [_1: string, _2: string]**

It has a total number of 2 columns and 3 rows.

- *SUCCESS*: Column _1 matches ^Hello
- *FAILURE*: Column _2 contains 1 row that does not match world$
""".strip()
        self.assertEqual(
            self.reporter.output_stream.get_output(),
            expected_output
        )

    def test_hasFunctionalDependency(self):
        df = self.sqlContext.createDataFrame([
            (1, 2, 1, 1),
            (9, 9, 9, 2),
            (9, 9, 9, 3)
        ])
        check = Check(df).hasFunctionalDependency(["_1", "_2"], ["_3"])
        check.run([self.reporter])
        expected_output = """
**Checking [_1: bigint, _2: bigint, _3: bigint, _4: bigint]**

It has a total number of 4 columns and 3 rows.

- *SUCCESS*: Column _3 is functionally dependent on _1, _2.
""".strip()
        self.assertEqual(
            self.reporter.output_stream.get_output(),
            expected_output
        )

    def test_hasForeignKey(self):
        base = self.sqlContext.createDataFrame([
            (1, 2, 3), (1, 2, 5), (1, 3, 3)
        ])
        ref = self.sqlContext.createDataFrame([
            (1, 2, 100), (1, 3, 100)
        ])
        columnTuple1 = ("_1", "_1")
        columnTuple2 = ("_2", "_2")
        check = Check(base).hasForeignKey(ref, columnTuple1, columnTuple2)
        check.run([self.reporter])
        expected_output = """
**Checking [_1: bigint, _2: bigint, _3: bigint]**

It has a total number of 3 columns and 3 rows.

- *SUCCESS*: Columns _1->_1, _2->_2 define a foreign key pointing to the reference table [_1: bigint, _2: bigint, _3: bigint].
""".strip()
        self.assertEqual(
            self.reporter.output_stream.get_output(),
            expected_output
        )

    def test_isJoinableWith(self):
        base = self.sqlContext.createDataFrame([
            (1, 2, 3), (1, 2, 5), (1, 3, 3)
        ])
        ref = self.sqlContext.createDataFrame([
            (1, 2, 100), (1, 3, 100)
        ])
        columnTuple1 = ("_1", "_1")
        columnTuple2 = ("_2", "_2")
        check = Check(base).isJoinableWith(ref, columnTuple1, columnTuple2)
        check.run([self.reporter])
        expected_output = """
**Checking [_1: bigint, _2: bigint, _3: bigint]**

It has a total number of 3 columns and 3 rows.

- *SUCCESS*: Key _1->_1, _2->_2 can be used for joining. Join columns cardinality in base table: 2. Join columns cardinality after joining: 2 (100.00%).
""".strip()
        self.assertEqual(
            self.reporter.output_stream.get_output(),
            expected_output
        )

    def test_satisfies(self):
        df = self.sqlContext.createDataFrame([
            (1, "a"), (2, "a"), (3, "a")
        ])
        check = Check(df).satisfies("_1 > 0").satisfies("_2 = 'a'")
        check.run([self.reporter])
        expected_output = """
**Checking [_1: bigint, _2: string]**

It has a total number of 2 columns and 3 rows.

- *SUCCESS*: Constraint _1 > 0 is satisfied.
- *SUCCESS*: Constraint _2 = 'a' is satisfied.
""".strip()
        self.assertEqual(
            self.reporter.output_stream.get_output(),
            expected_output
        )


if __name__ == '__main__':
    unittest.main()
