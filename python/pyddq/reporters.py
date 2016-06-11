import sys
from pyddq.streams import PrintStream, OutputStream, FileOutputStream


class PrintStreamReporter(object):
    def get_jvm_reporter(self, jvm, *args, **kwargs):
        raise NotImplementedError

    def __init__(self, output_stream=FileOutputStream(sys.stdout)):
        if not isinstance(output_stream, OutputStream):
            raise TypeError("output_stream should be a subclass of pyddq.streams.OutputStream")
        self.output_stream = output_stream


class MarkdownReporter(PrintStreamReporter):
    def get_jvm_reporter(self, jvm):
        print_stream = PrintStream(jvm, self.output_stream)
        return jvm.de.frosner.ddq.reporters.MarkdownReporter(
            print_stream.jvm_obj
        )


class ConsoleReporter(PrintStreamReporter):
    def get_jvm_reporter(self, jvm):
        print_stream = PrintStream(jvm, self.output_stream)
        return jvm.de.frosner.ddq.reporters.ConsoleReporter(
            print_stream.jvm_obj
        )
