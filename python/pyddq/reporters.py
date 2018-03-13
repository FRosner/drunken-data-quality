from pyddq.streams import PrintStream, OutputStream, ByteArrayOutputStream
from py4j.java_gateway import get_field
import pyddq.jvm_conversions as jc


class Reporter(object):
    def get_jvm_reporter(self, jvm, *args, **kwargs):
        raise NotImplementedError

class OutputStreamReporter(Reporter):
    def __init__(self, output_stream):
        if not isinstance(output_stream, OutputStream):
            raise TypeError("output_stream should be a subclass of pyddq.streams.OutputStream")
        self.output_stream = output_stream


class MarkdownReporter(OutputStreamReporter):
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


class ConsoleReporter(OutputStreamReporter):
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


class ZeppelinReporter(OutputStreamReporter):
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

class EmailReporter(Reporter):
    """
    A class which produces an HTML report of [[CheckResult]] and sends it to the
    configured SMTP server.
    Args:
        smtpServer (str): URL of the SMTP server to use for sending the email
        to (Set[str]): Email addresses of the receivers
        cc (Set[str]): Email addresses of the carbon copy receivers. Defaults to
            an empty set.
        subjectPrefix (str): Prefix to put in the email subject. Defaults to
            "Data Quality Report: ".
        smtpPort (int): Port of the SMTP server to use for sending the email.
            Defaults to 25.
        from_ (str): Email address of the sender. Defaults to "mail.ddq.io".
        usernameAndPassword (Tuple[str, str]):  Optional credentials. Defaults
            to None.
        reportOnlyOnFailure (bool): Whether to report only if there is a failing
            check (True) or always (False). Defaults to False.
        accumulatedReport (bool): Whether to report for each check result (False)
            or only when a report is triggered (True). The accumulated option
            requires the reporter to stick around until manually triggered or
            else you will lose the results. Defaults to False.
    """
    def __init__(self, smtpServer, to, cc=None, subjectPrefix=None,
                 smtpPort=None, from_=None, usernameAndPassword=None,
                 reportOnlyOnFailure=None, accumulatedReport=None):
        self._smtpServer = smtpServer
        self._to = to
        self._cc = cc
        self._subjectPrefix = subjectPrefix
        self._smtpPort = smtpPort
        self._from_ = from_
        self._usernameAndPassword = usernameAndPassword
        self._reportOnlyOnFailure = reportOnlyOnFailure
        self._accumulatedReport = accumulatedReport
        self._jvm_reporter = None

    @property
    def smtpServer(self):
        return self._smtpServer

    @property
    def to(self):
        return self._to

    @property
    def cc(self):
        return self._cc

    @property
    def subjectPrefix(self):
        return self._subjectPrefix

    @property
    def smtpPort(self):
        return self._smtpPort

    @property
    def from_(self):
        return self._from_

    @property
    def usernameAndPassword(self):
        return self._usernameAndPassword

    @property
    def reportOnlyOnFailure(self):
        return self._reportOnlyOnFailure

    @property
    def accumulatedReport(self):
        return self._accumulatedReport

    def get_jvm_reporter(self, jvm):
        if not self._jvm_reporter:
            self._jvm = jvm
            jvm_email_reporter = jvm.de.frosner.ddq.reporters.EmailReporter
            to = jc.iterable_to_scala_set(jvm, self.to)

            if self.cc is None:
                cc = getattr(jvm_email_reporter, "apply$default$3")()
            else:
                cc = jc.iterable_to_scala_set(jvm, self.cc)

            if self.subjectPrefix is None:
                subjectPrefix = getattr(jvm_email_reporter, "apply$default$4")()
            else:
                subjectPrefix = self.subjectPrefix

            if self.smtpPort is None:
                smtpPort = getattr(jvm_email_reporter, "apply$default$5")()
            else:
                smtpPort = self.smtpPort

            if self.from_ is None:
                from_ = getattr(jvm_email_reporter, "apply$default$6")()
            else:
                from_ = self.from_

            if self.usernameAndPassword is None:
                usernameAndPassword = getattr(jvm_email_reporter, "apply$default$7")()
            else:
                usernameAndPassword = jc.option(jvm, (jc.tuple2(jvm, self.usernameAndPassword)))

            if self.reportOnlyOnFailure is None:
                reportOnlyOnFailure = getattr(jvm_email_reporter, "apply$default$8")()
            else:
                reportOnlyOnFailure = self.reportOnlyOnFailure

            if self.accumulatedReport is None:
                accumulatedReport = getattr(jvm_email_reporter, "apply$default$9")()
            else:
                accumulatedReport = self.accumulatedReport

            self._jvm_reporter = jvm_email_reporter(
                self.smtpServer, to, cc, subjectPrefix, smtpPort, from_,
                usernameAndPassword, reportOnlyOnFailure, accumulatedReport
            )

        return self._jvm_reporter

    def sendAccumulatedReport(self, accumulatedCheckName=None):
        if not self._jvm_reporter:
            raise ValueError("No checks executed. Please run your checks before sending out a report.")

        if accumulatedCheckName is None:
            accumulatedCheckName = jc.scala_none(self._jvm)
        else:
            accumulatedCheckName = jc.option(self._jvm, accumulatedCheckName)

        self._jvm_reporter.sendAccumulatedReport(accumulatedCheckName)
