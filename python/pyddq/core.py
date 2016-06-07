from uuid import uuid4
from utils import iterableAsScalaList, iterableAsScalaSet, simpleDateFormat, tuple2
from reporters import ConsoleReporter

class Check(object):
    def __init__(self, dataFrame, displayName=None, jvmCheck=None):
        self._dataFrame = dataFrame
        self._jvm = dataFrame._sc._jvm
        self._displayName = displayName

        displayName = self._jvm.scala.Option.empty()
        cacheMethod = self._jvm.scala.Option.empty()
        constraints = self._jvm.scala.collection.immutable.List.empty()
        id = str(uuid4)
        if jvmCheck:
            self.jvmCheck = jvmCheck
        else:
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

    def hasUniqueKey(self, columnName, *columnNames):
        jvmCheck = self.jvmCheck.hasUniqueKey(
            columnName,
            iterableAsScalaList(self._jvm, columnNames)
        )
        return Check(self.dataFrame, self.displayName, jvmCheck)

    def isNeverNull(self, columnName):
        jvmCheck = self.jvmCheck.isNeverNull(columnName)
        return Check(self.dataFrame, self.displayName, jvmCheck)

    def isAlwaysNull(self, columnName):
        jvmCheck = self.jvmCheck.isAlwaysNull(columnName)
        return Check(self.dataFrame, self.displayName, jvmCheck)

    def isConvertibleTo(self, columnName, targetType):
        jvmType = self._jvm.org.apache.spark.sql.types.DataType.fromJson(
            targetType.json()
        )
        jvmCheck = self.jvmCheck.isConvertibleTo(columnName, jvmType)
        return Check(self.dataFrame, self.displayName, jvmCheck)

    def isFormattedAsDate(self, columnName, dateFormat):
        jvmFormat = simpleDateFormat(self._jvm, dateFormat)
        jvmCheck = self.jvmCheck.isFormattedAsDate(columnName, jvmFormat)
        return Check(self.dataFrame, self.displayName, jvmCheck)

    def isAnyOf(self, columnName, allowed):
        jvmCheck = self.jvmCheck.isAnyOf(
            columnName,
            iterableAsScalaSet(self._jvm, allowed)
        )
        return Check(self.dataFrame, self.displayName, jvmCheck)

    def isMatchingRegex(self, columnName, regexp):
        jvmCheck = self.jvmCheck.isMatchingRegex(columnName, regexp)
        return Check(self.dataFrame, self.displayName, jvmCheck)

    def hasFunctionalDependency(self, determinantSet, dependentSet):
        jvmCheck = self.jvmCheck.hasFunctionalDependency(
            iterableAsScalaList(self._jvm, determinantSet),
            iterableAsScalaList(self._jvm, dependentSet)
        )
        return Check(self.dataFrame, self.displayName, jvmCheck)

    def hasForeignKey(self, referenceTable, keyMap, *keyMaps):
        jvmCheck = self.jvmCheck.hasForeignKey(
            referenceTable._jdf,
            tuple2(self._jvm, keyMap),
            iterableAsScalaList(self._jvm,
                                map(lambda t: tuple2(self._jvm, t), keyMaps))
        )
        return Check(self.dataFrame, self.displayName, jvmCheck)

    def isJoinableWith(self, referenceTable, keyMap, *keyMaps):
        jvmCheck = self.jvmCheck.isJoinableWith(
            referenceTable._jdf,
            tuple2(self._jvm, keyMap),
            iterableAsScalaList(self._jvm,
                                map(lambda t: tuple2(self._jvm, t), keyMaps))
        )
        return Check(self.dataFrame, self.displayName, jvmCheck)

    def satisfies(self, constraint):
        jvmCheck = self.jvmCheck.satisfies(constraint)
        return Check(self.dataFrame, self.displayName, jvmCheck)

    def run(self, reporters=None):
        if not reporters:
            reporters = [ConsoleReporter()]

        jvmReporters = iterableAsScalaList(
            self._jvm,
            [reporter.getJvmReporter(self._jvm) for reporter in reporters]
        )
        return self.jvmCheck.run(jvmReporters)
