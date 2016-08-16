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
        """
        Sets JVMView used for creating java.io.OutputStream.
        Cannot be changed after the assignment
        Args:
            value (jvm y4j.java_gateway.JVMView)
        """
        if self._jvm:
            raise AttributeError("jvm is already set!")
        else:
            self._jvm = value


class FileOutputStream(OutputStream):
    """
    A wrapper around java.io.FileOutputStream
    Args:
        descriptor (file): open file descriptor to write the output.
            Supports sys.stdout and sys.stderr
    """
    def __init__(self, descriptor):
        if not isinstance(descriptor, file):
            raise ValueError("Descriptor is not a file")
        elif descriptor.closed:
            raise ValueError("Descriptor is closed")
        elif descriptor.mode == "r":
            raise ValueError("Descriptor is opened for reading")

        self.descriptor = descriptor

    @property
    def jvm_obj(self):
        """
        Returns underlying instance of java.io.FileOutputStream.
        Requires jvm attribute to be set
        """
        if not self._jvm_obj:
            stds = {
                "<stdout>": self.jvm.System.out,
                "<stderr>": self.jvm.System.err
            }

            if self.descriptor.name in stds:
                self._jvm_obj = stds[self.descriptor.name]
            else:
                self._jvm_obj = self.jvm.java.io.FileOutputStream(
                    self.descriptor.name,
                    "a" in self.descriptor.mode
                )
        return self._jvm_obj


class ByteArrayOutputStream(OutputStream):
    """
    A wrapper around java.io.ByteArrayOutputStream
    """
    @property
    def jvm_obj(self):
        """
        Returns underlying instance of java.io.ByteArrayOutputStream.
        Requires jvm attribute to be set
        """
        if not self._jvm_obj:
            self._jvm_obj = self.jvm.java.io.ByteArrayOutputStream()
        return self._jvm_obj

    def get_output(self):
        return self.jvm_obj.toString().strip()


class PrintStream(object):
    """
    A wrapper around java.io.PrintStream
    """
    def __init__(self, jvm, output_stream):
        self.output_stream = output_stream
        self.output_stream.jvm = jvm
        self.jvm_obj = jvm.java.io.PrintStream(output_stream.jvm_obj)
