from reporters import ConsoleReporter
from streams import ByteArrayOutputStream
import jvm_conversions as jc

from pyspark import sql


class Check(object):
    """
    A class representing a list of constraints that can be applied to a given
    pyspark.sql.dataframe.DataFrame.

    In order to run the checks, use the `run` method.
    """
    def __init__(self, dataFrame, displayName=None, cacheMethod=None,
                 id=None, jvmCheck=None):
        """
        Args:
            dataFrame (pyspark.sql.dataframe.DataFrame)
            displayName (str): The name to show in the logs. If it is not set,
                string representation of dataFrame will be used.
            cacheMethod (pyspark.storagelevel.StorageLevel): The StorageLevel to
                persist with before executing the checks.
        """
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
        """
        Checks whether the given columns are a unique key for this table.
        Args:
            columnName (str): name of the first column that is supposed to be
                part of the unique key
            columnNames (List[str]): names of the other columns that are
                supposed to be part of the unique key
        Returns:
            core.Check object including this constraint
        """
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
        """
        Checks whether the column with the given name contains no null values.
        Args:
            columnName (str): Name of the column to check
        Returns:
            core.Check object including this constraint
        """
        jvmCheck = self.jvmCheck.isNeverNull(columnName)
        return Check(
            self.dataFrame,
            self.name,
            self.cacheMethod,
            self.id,
            jvmCheck
        )

    def isAlwaysNull(self, columnName):
        """
        Checks whether the column with the given name contains only null values.
        Args:
            columnName (str): Name of the column to check
        Returns:
            core.Check object including this constraint
        """
        jvmCheck = self.jvmCheck.isAlwaysNull(columnName)
        return Check(
            self.dataFrame,
            self.name,
            self.cacheMethod,
            self.id,
            jvmCheck
        )

    def isConvertibleTo(self, columnName, targetType):
        """
        Checks whether the column with the given name can be converted to the
        given type.
        Args:
            columnName (str): Name of the column to check
            targetType (pyspark.sql.types.DataType): Type to try to convert to
        Returns:
            core.Check object including this constraint
        """
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
        """
        Checks whether the column with the given name can be converted to a date
        using the specified date format.
        Args:
            columnName (str): Name of the column to check
            dateFormat (str): Date format to use for conversion
        Returns:
            core.Check object including this constraint
        """
        jvmCheck = self.jvmCheck.isFormattedAsDate(columnName, dateFormat)
        return Check(
            self.dataFrame,
            self.name,
            self.cacheMethod,
            self.id,
            jvmCheck
        )

    def isAnyOf(self, columnName, allowed):
        """
        Checks whether the column with the given name is always any of the
        specified values.
        Args:
            columnName (str): Name of the column to check
            allowed (List): allowed values
        Returns:
            core.Check object including this constraint
        """
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
        """
        Checks whether the column with the given name is always matching the
        specified regular expression.
        Args:
            columnName (str): Name of the column to check
            regex (str): Regular expression that needs to match
        Returns:
            core.Check object including this constraint
        """
        jvmCheck = self.jvmCheck.isMatchingRegex(columnName, regexp)
        return Check(
            self.dataFrame,
            self.name,
            self.cacheMethod,
            self.id,
            jvmCheck
        )

    def hasFunctionalDependency(self, determinantSet, dependentSet):
        """
        Checks whether the columns in the dependent set have a functional
        dependency on determinant set.
        Args:
            determinantSet (List[str]): column names which form a determinant
                set
            dependentSet (List[str]): sequence of column names which form a
                dependent set
        Returns:
             core.Check object including this constraint
        """
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
        """
        Checks whether the columns with the given names define a foreign key to
        the specified reference table.
        Args:
            referenceTable (pyspark.sql.dataframe.DataFrame): Table to which the
                foreign key is pointing
            keyMap (Tuple[str, str]): Column mapping from this table to the
                reference one ("column1", "base_column1")
            keyMaps (Tuple[str, str]):  Column mappings from this table to the
                reference one ("column1", "base_column1")
        Returns:
        core.Check object including this constraint
        """
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
        """
        Checks whether a join between this table and the given reference table
        returns any results. This can be seen as a weaker version of the foreign
        key check, as it requires only partial matches.
        Args:
            referenceTable (pyspark.sql.dataframe.DataFrame): Table to which the
                foreign key is pointing
            keyMap (Tuple[str, str]): Column mapping from this table to the
                reference one ("column1", "base_column1")
            keyMaps (Tuple[str, str]):  Column mappings from this table to the
                reference one ("column1", "base_column1")
        Returns:
        core.Check object including this constraint
        """
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
        """
        Checks whether the given constraint is satisfied. The constraint has to
        comply with Spark SQL syntax. So you can just write it the same way that
        you would put it inside a `WHERE` clause.
        Args:
            constraint (Union[str, pyspark.sql.Column]): The constraint that needs to be satisfied for all
                columns
        Returns:
        core.Check object including this constraint
        """
        if isinstance(constraint, str):
            jvmCheck = self.jvmCheck.satisfies(constraint)
        elif isinstance(constraint, sql.Column):
            jvmCheck = self.jvmCheck.satisfies(constraint._jc)
        else:
            raise ValueError("constraint can be either str or pyspark.sql.Column, got %s" % type(constraint))

        return Check(
            self.dataFrame,
            self.name,
            self.cacheMethod,
            self.id,
            jvmCheck
        )

    def isEqualTo(self, other):
        """
        Checks whether the other dataframe is exactly equal to this one. Equality
        checks will be performed on a column basis depending on the column type using the Spark SQL
        equality operator.
        Comparison will be executed in a distributed way so it might take a while.
        Args:
            other (pyspark.sql.dataframe.DataFrame): other data set to compare with
        Returns:
        core.Check object including this constraint
        """
        jvmCheck = self.jvmCheck.isEqualTo(other._jdf)
        return Check(
            self.dataFrame,
            self.name,
            self.cacheMethod,
            self.id,
            jvmCheck
        )

    def run(self, reporters=None):
        """
        Runs check with all the previously specified constraints and report to
        every reporter passed as an argument
        Args:
            reporters (List[reporters.Reporter]): iterable of reporters
                to produce output on the check result. If not specified,
                reporters.ConsoleReporter is used
        Returns: None
        """
        baos = None
        if not reporters:
            baos = ByteArrayOutputStream()
            reporters = [ConsoleReporter(baos)]

        jvm_reporters = jc.iterable_to_scala_list(
            self._jvm,
            [reporter.get_jvm_reporter(self._jvm) for reporter in reporters]
        )
        self.jvmCheck.run(jvm_reporters)

        if baos:
            print baos.get_output()
