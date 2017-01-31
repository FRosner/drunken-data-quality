import unittest
from mock import Mock, PropertyMock, patch
import pyddq.reporters as r
import pyddq.streams as s


class ReporterTest(unittest.TestCase):
    def test_constructor(self):
        self.assertRaises(TypeError, r.Reporter, 10)


class ConsoleReporterTest(unittest.TestCase):
    def test_get_jvm_reporter(self):
        output_stream = s.OutputStream()
        with patch(
                "pyddq.streams.OutputStream.jvm_obj",
                new_callable=PropertyMock
        ):
            console_reporter = Mock()
            jvm = Mock()
            jvm.de.frosner.ddq.reporters.ConsoleReporter = Mock(
                return_value=console_reporter
            )

            reporter = r.ConsoleReporter(output_stream)
            jvm_reporter = reporter.get_jvm_reporter(jvm)
            self.assertEqual(
                jvm_reporter,
                console_reporter
            )


class MarkdownReporterTest(unittest.TestCase):
    def test_get_jvm_reporter(self):
        output_stream = s.OutputStream()
        with patch(
                "pyddq.streams.OutputStream.jvm_obj",
                new_callable=PropertyMock
        ):
            markdown_reporter = Mock()
            jvm = Mock()
            jvm.de.frosner.ddq.reporters.MarkdownReporter = Mock(
                return_value = markdown_reporter
            )

            reporter = r.MarkdownReporter(output_stream)
            jvm_reporter = reporter.get_jvm_reporter(jvm)
            self.assertEqual(
                jvm_reporter,
                markdown_reporter
            )


class ZeppelinReporter(unittest.TestCase):
    def test_get_jvm_reporter(self):
        with patch("pyddq.reporters.get_field") as get_field:
            jvm = Mock()

            ddq_zeppelin_reporter = Mock()
            jvm.de.frosner.ddq.reporters.ZeppelinReporter = Mock(
                return_value=ddq_zeppelin_reporter
            )

            jvm_print_stream = Mock()
            jvm.java.io.PrintStream = Mock(
                return_value=jvm_print_stream
            )

            output_stream = Mock()
            get_field.return_value = output_stream
            z = Mock()
            print ZeppelinReporter
            reporter = r.ZeppelinReporter(z)
            jvm_reporter = reporter.get_jvm_reporter(jvm)
            # check that proper ddq reporter is returned
            self.assertEqual(
                jvm_reporter,
                ddq_zeppelin_reporter
            )
            # check that proper printstream is passed to the reporter
            jvm.de.frosner.ddq.reporters.ZeppelinReporter.assert_called_with(
                jvm_print_stream
            )
            # check that the printstream is constructed with a proper outputstream
            jvm.java.io.PrintStream.assert_called_with(output_stream)


class EmailReporter(unittest.TestCase):
    def test_get_jvm_reporter(self):
        jvm = Mock()
        ddq_email_reporter = Mock()
        jvm.de.frosner.ddq.reporters.EmailReporter = Mock(
            return_value = ddq_email_reporter
        )
        reporter = r.EmailReporter("smtp", "to")
        jvm_reporter = reporter.get_jvm_reporter(jvm)
        self.assertEqual(
            jvm_reporter,
            ddq_email_reporter
        )

    def test_send_accumulated_report(self):
        jvm = Mock()
        ddq_email_reporter = Mock()
        jvm.de.frosner.ddq.reporters.EmailReporter = Mock(
            return_value = ddq_email_reporter
        )
        reporter = r.EmailReporter("smtp", "to")
        self.assertRaises(ValueError, reporter.sendAccumulatedReport)

        reporter.get_jvm_reporter(jvm) # usually called by Check.run
        reporter.sendAccumulatedReport()

        self.assertTrue(ddq_email_reporter.sendAccumulatedReport.called)


if __name__ == '__main__':
    unittest.main()
