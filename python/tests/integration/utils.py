class DummyReporter(object):
    def _get_byte_array_output_stream(self, jvm):
        return jvm.java.io.ByteArrayOutputStream()

    def _get_print_stream_constructor(self, baos):
        return lambda jvm: jvm.java.io.PrintStream(baos)

    def __init__(self):
        self.baos = None

    def get_output(self):
        return self.baos.toString().strip()

    def get_jvm_reporter(self, jvm):
        self.baos = self._get_byte_array_output_stream(jvm)
        print_stream = self._get_print_stream_constructor(self.baos)(jvm)
        return jvm.de.frosner.ddq.reporters.MarkdownReporter(print_stream)
