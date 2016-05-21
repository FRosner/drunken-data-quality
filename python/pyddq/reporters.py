import sys


class PrintStreamReporter(object):
    def _getPrintStreamConstructor(self, descriptor):
        mode = descriptor.mode
        if mode == "r":
            raise ValueError("Descriptor is opened for reading")

        defaultMapper = {
            "<stdout>": lambda jvm: jvm.System.out,
            "<stderr>": lambda jvm: jvm.System.err
        }

        psConstructor = defaultMapper.get(descriptor.name)
        if not psConstructor:
            psConstructor = lambda jvm: jvm.java.io.PrintStream(
                jvm.java.io.FileOutputStream(
                    descriptor.name,
                    "a" in mode)
            )
        return psConstructor


    def getJvmReporter(self, jvm, *args, **kwargs):
        raise NotImplementedError

    def __init__(self, descriptor=sys.stdout):
        self._psConstructor = self._getPrintStreamConstructor(descriptor)

class MarkdownReporter(PrintStreamReporter):
    def getJvmReporter(self, jvm):
        printStream = self._psConstructor(jvm)
        return jvm.de.frosner.ddq.reporters.MarkdownReporter(printStream)


class ConsoleReporter(PrintStreamReporter):
    def getJvmReporter(self, jvm):
        printStream = self._psConstructor(jvm)
        return jvm.de.frosner.ddq.reporters.ConsoleReporter(printStream)
