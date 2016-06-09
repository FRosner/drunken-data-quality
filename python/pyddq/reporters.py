import sys


class PrintStreamReporter(object):
    def _get_print_stream_constructor(self, descriptor):
        mode = descriptor.mode
        if mode == "r":
            raise ValueError("Descriptor is opened for reading")

        default_mapper = {
            "<stdout>": lambda jvm: jvm.System.out,
            "<stderr>": lambda jvm: jvm.System.err
        }

        ps_constructor = default_mapper.get(descriptor.name)
        if not ps_constructor:
            ps_constructor = lambda jvm: jvm.java.io.PrintStream(
                jvm.java.io.FileOutputStream(
                    descriptor.name,
                    "a" in mode)
            )
        return ps_constructor


    def get_jvm_reporter(self, jvm, *args, **kwargs):
        raise NotImplementedError

    def __init__(self, descriptor=sys.stdout):
        self._ps_constructor = self._get_print_stream_constructor(descriptor)


class MarkdownReporter(PrintStreamReporter):
    def get_jvm_reporter(self, jvm):
        print_stream = self._ps_constructor(jvm)
        return jvm.de.frosner.ddq.reporters.MarkdownReporter(print_stream)


class ConsoleReporter(PrintStreamReporter):
    def get_jvm_reporter(self, jvm):
        print_stream = self._ps_constructor(jvm)
        return jvm.de.frosner.ddq.reporters.ConsoleReporter(print_stream)
