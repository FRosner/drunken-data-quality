import unittest

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import types as t

from utils import DummyReporter
from pyddq.core import Check
from pyddq.reporters import MarkdownReporter


class ConstraintTest(unittest.TestCase):
    def setUp(self):
        self.sc = SparkContext()
        self.sqlContext = SQLContext(self.sc)

    def tearDown(self):
        self.sc.stop()

    def testHasUniqueKey(self):
        df = self.sqlContext.createDataFrame([(1, "a"), (1, None), (3, "c")])
        check = Check(df).hasUniqueKey("_1").hasUniqueKey("_1", "_2")
        reporter = DummyReporter()
        check.run([reporter])
        expectedOutput = """
**Checking [_1: bigint, _2: string]**

It has a total number of 2 columns and 3 rows.

- *FAILURE*: Column _1 is not a key (1 non-unique tuple).
- *SUCCESS*: Columns _1, _2 are a key.
""".strip()
        self.assertEqual(reporter.getOutput(), expectedOutput)

    def testIsNeverNull(self):
        df = self.sqlContext.createDataFrame([(1, "a"), (1, None), (3, "c")])
        check = Check(df).isNeverNull("_1").isNeverNull("_2")
        reporter = DummyReporter()
        check.run([reporter])
        expectedOutput = """
**Checking [_1: bigint, _2: string]**

It has a total number of 2 columns and 3 rows.

- *SUCCESS*: Column _1 is never null.
- *FAILURE*: Column _2 contains 1 row that is null (should never be null).
""".strip()
        self.assertEqual(reporter.getOutput(), expectedOutput)

    def testIsAlwaysNull(self):
        schema = t.StructType([
            t.StructField("_1", t.IntegerType()),
            t.StructField("_2", t.StringType()),
        ])
        df = self.sqlContext.createDataFrame(
            [(1, None), (1, None), (3, None)],
            schema
        )
        check = Check(df).isAlwaysNull("_1").isAlwaysNull("_2")
        reporter = DummyReporter()
        check.run([reporter])
        expectedOutput = """
**Checking [_1: int, _2: string]**

It has a total number of 2 columns and 3 rows.

- *FAILURE*: Column _1 contains 3 non-null rows (should always be null).
- *SUCCESS*: Column _2 is always null.
""".strip()
        self.assertEqual(reporter.getOutput(), expectedOutput)

    def testIsConvertibleTo(self):
        df = self.sqlContext.createDataFrame([(1, "a"), (1, None), (3, "c")])
        check = Check(df)\
                .isConvertibleTo("_1", t.IntegerType())\
                .isConvertibleTo("_1", t.ArrayType(t.IntegerType()))
        reporter = DummyReporter()
        check.run([reporter])
        expectedOutput = """
**Checking [_1: bigint, _2: string]**

It has a total number of 2 columns and 3 rows.

- *SUCCESS*: Column _1 can be converted from LongType to IntegerType.
- *ERROR*: Checking whether column _1 can be converted to ArrayType(IntegerType,true) failed: org.apache.spark.sql.AnalysisException: cannot resolve 'cast(_1 as array<int>)' due to data type mismatch: cannot cast LongType to ArrayType(IntegerType,true);
""".strip()
        self.assertEqual(reporter.getOutput(), expectedOutput)

    def testIsFormattedAsDate(self):
        df = self.sqlContext.createDataFrame([
            ("2000-11-23 11:50:10", ),
            ("2000-5-23 11:50:10", ),
            ("2000-02-23 11:11:11", )
        ])
        check = Check(df).isFormattedAsDate("_1", "yyyy-MM-dd HH:mm:ss")
        reporter = DummyReporter()
        check.run([reporter])
        expectedOutput = """
**Checking [_1: string]**

It has a total number of 1 columns and 3 rows.

- *SUCCESS*: Column _1 is formatted by yyyy-MM-dd HH:mm:ss.
""".strip()

    def testIsAnyOf(self):
        df = self.sqlContext.createDataFrame([(1, "a"), (2, "b"), (3, "c")])
        check = Check(df).isAnyOf("_1", [1, 2]).isAnyOf("_2", ["a", "b", "c"])
        reporter = DummyReporter()
        check.run([reporter])
        expectedOutput = """
**Checking [_1: bigint, _2: string]**

It has a total number of 2 columns and 3 rows.

- *FAILURE*: Column _1 contains 1 row that is not in Set(1, 2).
- *SUCCESS*: Column _2 contains only values in Set(a, b, c).
""".strip()
        self.assertEqual(reporter.getOutput(), expectedOutput)

    def testIsMatchingRegex(self):
        df = self.sqlContext.createDataFrame([
            ("Hello A", "world"),
            ("Hello B", None),
            ("Hello C", "World")
        ])
        check = Check(df)\
                .isMatchingRegex("_1", "^Hello")\
                .isMatchingRegex("_2", "world$")

        reporter = DummyReporter()
        check.run([reporter])
        expectedOutput = """
**Checking [_1: string, _2: string]**

It has a total number of 2 columns and 3 rows.

- *SUCCESS*: Column _1 matches ^Hello
- *FAILURE*: Column _2 contains 1 row that does not match world$
""".strip()
        self.assertEqual(reporter.getOutput(), expectedOutput)

    def testHasFunctionalDependency(self):
        df = self.sqlContext.createDataFrame([
            (1, 2, 1, 1),
            (9, 9, 9, 2),
            (9, 9, 9, 3)
        ])
        check = Check(df).hasFunctionalDependency(["_1", "_2"], ["_3"])
        reporter = DummyReporter()
        check.run([reporter])
        expectedOutput = """
**Checking [_1: bigint, _2: bigint, _3: bigint, _4: bigint]**

It has a total number of 4 columns and 3 rows.

- *SUCCESS*: Column _3 is functionally dependent on _1, _2.
""".strip()
        self.assertEqual(reporter.getOutput(), expectedOutput)


if __name__ == '__main__':
    unittest.main()
