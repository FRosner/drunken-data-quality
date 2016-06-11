class OutputStream(object):
    _jvm = None
    _jvm_obj = None

    @property
    def jvm_obj(self):
        raise NotImplementedError

    @property
    def jvm(self):
        if not self._jvm:
            raise AttributeError("jvm is not yet set!")
        else:
            return self._jvm

    @jvm.setter
    def jvm(self, value):
        if self._jvm:
            raise AttributeError("jvm is already set!")
        else:
            self._jvm = value


class FileOutputStream(OutputStream):
    def __init__(self, descriptor):
        self.descriptor = descriptor
        mode = descriptor.mode
        if mode == "r":
            raise ValueError("Descriptor is opened for reading")

    @property
    def jvm_obj(self):
        self.jvm  # check that jvm is available and does not raise AttributeError

        if not self._jvm_obj:
            stds = {
                "<stdout>": self.jvm.System.out,
                "<stderr>": self.jvm.System.err
            }

            if self.descriptor.name in stds:
                self._jvm_obj = stds[self.descriptor.name]
            else:
                self._jvm_obj =  self.jvm.java.io.FileOutputStream(
                    self.descriptor.name,
                    "a" in self.descriptor.mode
                )
        return self._jvm_obj


class ByteArrayOutputStream(OutputStream):
    @property
    def jvm_obj(self):
        self.jvm  # check that jvm is available and does not raise AttributeError
        if not self._jvm_obj:
            self._jvm_obj = self.jvm.java.io.ByteArrayOutputStream()
        return self._jvm_obj

    def get_output(self):
        return self.jvm_obj.toString().strip()


class PrintStream(object):
    def __init__(self, jvm, output_stream):
        self.output_stream = output_stream
        self.output_stream.jvm = jvm
        self.jvm_obj = jvm.java.io.PrintStream(output_stream.jvm_obj)
