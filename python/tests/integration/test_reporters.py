import unittest
from mock import Mock, patch

from pyspark import SparkContext, StorageLevel
from pyspark.sql import SQLContext

from pyddq.core import Check
from pyddq.reporters import ConsoleReporter, MarkdownReporter, ZeppelinReporter, EmailReporter
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


class ZeppelinReporterTest(unittest.TestCase):
    def setUp(self):
        self.sc = SparkContext()
        self.sql = SQLContext(self.sc)
        self.df = self.sql.createDataFrame([(1, "a"), (1, None), (3, "c")])

    def tearDown(self):
        self.sc.stop()

    def test_output(self):
        with patch("pyddq.reporters.get_field") as get_field:
            baos = ByteArrayOutputStream()
            baos.jvm = self.df._sc._jvm

            get_field.return_value = baos.jvm_obj
            check = Check(self.df).hasUniqueKey("_1").hasUniqueKey("_1", "_2")
            z = Mock()
            reporter = ZeppelinReporter(z)
            check.run([reporter])
            expected_output = """
%html
</p>
<h4>Checking [_1: bigint, _2: string]</h4>
<h5>It has a total number of 2 columns and 3 rows.</h5>
<table>
<tr><td style="padding:3px">&#10060;</td><td style="padding:3px">Column _1 is not a key (1 non-unique tuple).</td></tr>
<tr><td style="padding:3px">&#9989;</td><td style="padding:3px">Columns _1, _2 are a key.</td></tr>
</table>
<p hidden>
""".strip()
            self.assertEqual(baos.get_output(), expected_output)

class EmailReporterTest(unittest.TestCase):
    def setUp(self):
        self.sc = SparkContext()
        self.sql = SQLContext(self.sc)
        self.df = self.sql.createDataFrame([(1, "a"), (1, None), (3, "c")])

    def tearDown(self):
        self.sc.stop()

    def test_default_arguments(self):
        check = Check(self.df).hasUniqueKey("_1").hasUniqueKey("_1", "_2")
        reporter = EmailReporter("test@smtp.de", {"test@receivers.de"})
        check.run([reporter])

    def test_passed_arguments(self):
        check = Check(self.df).hasUniqueKey("_1").hasUniqueKey("_1", "_2")
        smtpServer = "test@smtp.de"
        to = {"test@receivers.de"}
        cc = {"test@cced.de"}
        subjectPrefix = "my subject prefix: "
        smtpPort = 9000
        from_ = "test.ddq.io"
        usernameAndPassword = ("username", "password")
        reportOnlyOnFailure = True
        accumulatedReport = True
        reporter = EmailReporter(
            smtpServer, to, cc, subjectPrefix, smtpPort, from_,
            usernameAndPassword, reportOnlyOnFailure, accumulatedReport
        )
        check.run([reporter])

    def test_accumulated_report(self):
        check = Check(self.df).hasUniqueKey("_1").hasUniqueKey("_1", "_2")
        reporter = EmailReporter("test@smtp.de", {"test@receivers.de"}, accumulatedReport=True)
        check.run([reporter])
        reporter.sendAccumulatedReport()
        reporter.sendAccumulatedReport("111")

if __name__ == '__main__':
    unittest.main()
