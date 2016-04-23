from uuid import uuid4
from utils import iterableAsScalaList

class Check(object):
    def __init__(self, dataFrame, displayName=None):
        self._dataFrame = dataFrame
        self._jvm = dataFrame._sc._jvm
        self._displayName = displayName

        displayName = self._jvm.scala.Option.empty()
        cacheMethod = self._jvm.scala.Option.empty()
        constraints = self._jvm.scala.collection.immutable.List.empty()
        id = str(uuid4)
        self.jvmCheck = self._jvm.de.frosner.ddq.core.Check(dataFrame._jdf,
                                                            displayName,
                                                            cacheMethod,
                                                            constraints,
                                                            id)
    @property
    def dataFrame(self):
        return self._dataFrame

    @property
    def displayName(self):
        return self._displayName or str(self.dataFrame)

    def isNeverNull(self, columnName):
        self.jvmCheck = self.jvmCheck.isNeverNull(columnName)
        return self

    def run(self, reporters):
        jvm_reporters = iterableAsScalaList(
            self._jvm,
            [reporter.get_jvm_reporter(self._jvm) for reporter in reporters]
        )
        return self.jvmCheck.run(jvm_reporters)
