import unittest
from mock import Mock

from pyspark import SparkContext, StorageLevel
from pyspark.sql import SQLContext

from pyddq.core import Check
from pyddq.reporters import ConsoleReporter, MarkdownReporter
from pyddq.streams import ByteArrayOutputStream


class ConsoleReporterTest(unittest.TestCase):
    def setUp(self):
        self.sc = SparkContext()
        self.sql = SQLContext(self.sc)
        self.df = self.sql.createDataFrame([(1, "a"), (1, None), (3, "c")])

    def tearDown(self):
        self.sc.stop()

    def test_output(self):
        check = Check(self.df).hasUniqueKey("_1").hasUniqueKey("_1", "_2")
        baos = ByteArrayOutputStream()
        reporter = ConsoleReporter(baos)
        check.run([reporter])
        expected_output = """
\x1b[34mChecking [_1: bigint, _2: string]\x1b[0m
\x1b[34mIt has a total number of 2 columns and 3 rows.\x1b[0m
\x1b[31m- Column _1 is not a key (1 non-unique tuple).\x1b[0m
\x1b[32m- Columns _1, _2 are a key.\x1b[0m
""".strip()
        self.assertEqual(baos.get_output(), expected_output)


class MarkdownReporterTest(unittest.TestCase):
    def setUp(self):
        self.sc = SparkContext()
        self.sql = SQLContext(self.sc)
        self.df = self.sql.createDataFrame([(1, "a"), (1, None), (3, "c")])

    def tearDown(self):
        self.sc.stop()

    def test_output(self):
        check = Check(self.df).hasUniqueKey("_1").hasUniqueKey("_1", "_2")
        baos = ByteArrayOutputStream()
        reporter = MarkdownReporter(baos)
        check.run([reporter])
        expected_output = """
**Checking [_1: bigint, _2: string]**

It has a total number of 2 columns and 3 rows.

- *FAILURE*: Column _1 is not a key (1 non-unique tuple).
- *SUCCESS*: Columns _1, _2 are a key.
""".strip()
        self.assertEqual(baos.get_output(), expected_output)


if __name__ == '__main__':
    unittest.main()
