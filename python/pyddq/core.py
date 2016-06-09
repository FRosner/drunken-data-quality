from reporters import ConsoleReporter
import jvm_conversions as jc


class Check(object):
    def __init__(self, dataFrame, displayName=None, cacheMethod=None,
                 id=None, jvmCheck=None):
        self._jvm = dataFrame._sc._jvm
        self._dataFrame = dataFrame
        self._displayName = displayName
        self._cacheMethod = cacheMethod
        self._id = id
        if jvmCheck:
            self.jvmCheck = jvmCheck
        else:
            ddq_check = self._jvm.de.frosner.ddq.core.Check
            self.jvmCheck = ddq_check(
                self._dataFrame._jdf,
                self._jvm_display_name,
                self._jvm_cache_method,
                getattr(ddq_check, "apply$default$4")(),
                self._id or getattr(ddq_check, "apply$default$5")()
            )

    @property
    def _jvm_display_name(self):
        if self._displayName:
            return self._jvm.scala.Some.apply(self._displayName)
        else:
            return getattr(
                self._jvm.de.frosner.ddq.core.Check,
                "apply$default$2"
            )()

    @property
    def _jvm_cache_method(self):
        if self._cacheMethod:
            return self._jvm.scala.Some.apply(
                self._jvm.org.apache.spark.storage.StorageLevel(
                    self._cacheMethod.useDisk,
                    self._cacheMethod.useMemory,
                    self._cacheMethod.useOffHeap,
                    self._cacheMethod.deserialized,
                    self._cacheMethod.replication
                )
            )
        else:
            return getattr(
                self._jvm.de.frosner.ddq.core.Check,
                "apply$default$3"
            )()

    @property
    def dataFrame(self):
        return self._dataFrame

    @property
    def name(self):
        return self._displayName or str(self.dataFrame)

    @property
    def cacheMethod(self):
        return self._cacheMethod

    @property
    def id(self):
        return self._id or str(self.jvmCheck.id())

    def hasUniqueKey(self, columnName, *columnNames):
        jvmCheck = self.jvmCheck.hasUniqueKey(
            columnName,
            jc.iterable_to_scala_list(self._jvm, columnNames)
        )
        return Check(
            self.dataFrame,
            self.name,
            self.cacheMethod,
            self.id,
            jvmCheck
        )

    def isNeverNull(self, columnName):
        jvmCheck = self.jvmCheck.isNeverNull(columnName)
        return Check(
            self.dataFrame,
            self.name,
            self.cacheMethod,
            self.id,
            jvmCheck
        )

    def isAlwaysNull(self, columnName):
        jvmCheck = self.jvmCheck.isAlwaysNull(columnName)
        return Check(
            self.dataFrame,
            self.name,
            self.cacheMethod,
            self.id,
            jvmCheck
        )

    def isConvertibleTo(self, columnName, targetType):
        jvmType = self._jvm.org.apache.spark.sql.types.DataType.fromJson(
            targetType.json()
        )
        jvmCheck = self.jvmCheck.isConvertibleTo(columnName, jvmType)
        return Check(
            self.dataFrame,
            self.name,
            self.cacheMethod,
            self.id,
            jvmCheck
        )

    def isFormattedAsDate(self, columnName, dateFormat):
        jvm_format = jc.simple_date_format(self._jvm, dateFormat)
        jvmCheck = self.jvmCheck.isFormattedAsDate(columnName, jvm_format)
        return Check(
            self.dataFrame,
            self.name,
            self.cacheMethod,
            self.id,
            jvmCheck
        )

    def isAnyOf(self, columnName, allowed):
        jvmCheck = self.jvmCheck.isAnyOf(
            columnName,
            jc.iterable_to_scala_set(self._jvm, allowed)
        )
        return Check(
            self.dataFrame,
            self.name,
            self.cacheMethod,
            self.id,
            jvmCheck
        )

    def isMatchingRegex(self, columnName, regexp):
        jvmCheck = self.jvmCheck.isMatchingRegex(columnName, regexp)
        return Check(
            self.dataFrame,
            self.name,
            self.cacheMethod,
            self.id,
            jvmCheck
        )

    def hasFunctionalDependency(self, determinantSet, dependentSet):
        jvmCheck = self.jvmCheck.hasFunctionalDependency(
            jc.iterable_to_scala_list(self._jvm, determinantSet),
            jc.iterable_to_scala_list(self._jvm, dependentSet)
        )
        return Check(
            self.dataFrame,
            self.name,
            self.cacheMethod,
            self.id,
            jvmCheck
        )

    def hasForeignKey(self, referenceTable, keyMap, *keyMaps):
        jvmCheck = self.jvmCheck.hasForeignKey(
            referenceTable._jdf,
            jc.tuple2(self._jvm, keyMap),
            jc.iterable_to_scala_list(
                self._jvm,
                map(lambda t: jc.tuple2(self._jvm, t), keyMaps)
            )
        )
        return Check(
            self.dataFrame,
            self.name,
            self.cacheMethod,
            self.id,
            jvmCheck
        )

    def isJoinableWith(self, referenceTable, keyMap, *keyMaps):
        jvmCheck = self.jvmCheck.isJoinableWith(
            referenceTable._jdf,
            jc.tuple2(self._jvm, keyMap),
            jc.iterable_to_scala_list(
                self._jvm,
                map(lambda t: jc.tuple2(self._jvm, t), keyMaps)
            )
        )
        return Check(
            self.dataFrame,
            self.name,
            self.cacheMethod,
            self.id,
            jvmCheck
        )

    def satisfies(self, constraint):
        jvmCheck = self.jvmCheck.satisfies(constraint)
        return Check(
            self.dataFrame,
            self.name,
            self.cacheMethod,
            self.id,
            jvmCheck
        )

    def run(self, reporters=None):
        if not reporters:
            reporters = [ConsoleReporter()]

        jvm_reporters = jc.iterable_to_scala_list(
            self._jvm,
            [reporter.get_jvm_reporter(self._jvm) for reporter in reporters]
        )
        return self.jvmCheck.run(jvm_reporters)
