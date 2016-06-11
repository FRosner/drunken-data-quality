import unittest
from mock import Mock, PropertyMock, patch

from pyddq.reporters import PrintStreamReporter, ConsoleReporter, MarkdownReporter
from pyddq.streams import OutputStream

class PrintStreamReporterTest(unittest.TestCase):
    def test_constructor(self):
        self.assertRaises(TypeError, PrintStreamReporter, 10)


class ConsoleReporterTest(unittest.TestCase):
    def test_get_jvm_reporter(self):
        output_stream = OutputStream()
        with patch(
                "pyddq.streams.OutputStream.jvm_obj",
                new_callable=PropertyMock
        ):
            console_reporter = Mock()
            jvm = Mock()
            jvm.de.frosner.ddq.reporters.ConsoleReporter = Mock(
                return_value=console_reporter
            )

            reporter = ConsoleReporter(output_stream)
            jvm_reporter = reporter.get_jvm_reporter(jvm)
            self.assertEqual(
                jvm_reporter,
                console_reporter
            )


class MarkdownReporterTest(unittest.TestCase):
    def test_get_jvm_reporter(self):
        output_stream = OutputStream()
        with patch(
                "pyddq.streams.OutputStream.jvm_obj",
                new_callable=PropertyMock
        ):
            markdown_reporter = Mock()
            jvm = Mock()
            jvm.de.frosner.ddq.reporters.MarkdownReporter = Mock(
                return_value = markdown_reporter
            )

            reporter = MarkdownReporter(output_stream)
            jvm_reporter = reporter.get_jvm_reporter(jvm)
            self.assertEqual(
                jvm_reporter,
                markdown_reporter
            )


if __name__ == '__main__':
    unittest.main()
