from pyddq.streams import PrintStream, OutputStream, ByteArrayOutputStream
from py4j.java_gateway import get_field


class Reporter(object):
    def get_jvm_reporter(self, jvm, *args, **kwargs):
        raise NotImplementedError

    def __init__(self, output_stream):
        if not isinstance(output_stream, OutputStream):
            raise TypeError("output_stream should be a subclass of pyddq.streams.OutputStream")
        self.output_stream = output_stream


class MarkdownReporter(Reporter):
    """
    A class which produces a markdown report of core.Check.run
    Args:
        output_stream (streams.OutputStream)
    """
    def get_jvm_reporter(self, jvm):
        print_stream = PrintStream(jvm, self.output_stream)
        return jvm.de.frosner.ddq.reporters.MarkdownReporter(
            print_stream.jvm_obj
        )


class ConsoleReporter(Reporter):
    """
    A class which produces a console report of core.Check.run
    Args:
        output_stream (streams.OutputStream)
    """
    def get_jvm_reporter(self, jvm):
        print_stream = PrintStream(jvm, self.output_stream)
        return jvm.de.frosner.ddq.reporters.ConsoleReporter(
            print_stream.jvm_obj
        )


class ZeppelinReporter(Reporter):
    """
    A class which produces a report of core.Check.run in a Zeppelin notebook note.
    Args:
        zeppelin_context (PyZeppelinContext): It is defined as "z" in a Zeppelin
            notebook.
    """
    def __init__(self, zeppelin_context):
        self.zeppelin_context = zeppelin_context

    def get_jvm_reporter(self, jvm):
        jvm_zeppelin_context = self.zeppelin_context.z
        jvm_intp_context = jvm_zeppelin_context.getInterpreterContext()
        jvm_output_stream = get_field(jvm_intp_context, "out")
        jvm_print_stream = jvm.java.io.PrintStream(jvm_output_stream)
        return jvm.de.frosner.ddq.reporters.ZeppelinReporter(
            jvm_print_stream
        )
