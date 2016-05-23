class DummyReporter(object):
    def _getByteArrayOutputStream(self, jvm):
        return jvm.java.io.ByteArrayOutputStream()

    def _getPrintStreamConstructor(self, baos):
        return lambda jvm: jvm.java.io.PrintStream(baos)

    def __init__(self):
        self.baos = None

    def getOutput(self):
        return self.baos.toString().strip()

    def getJvmReporter(self, jvm):
        self.baos = self._getByteArrayOutputStream(jvm)
        printStream = self._getPrintStreamConstructor(self.baos)(jvm)
        return jvm.de.frosner.ddq.reporters.MarkdownReporter(printStream)
