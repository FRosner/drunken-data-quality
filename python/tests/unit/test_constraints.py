import unittest
from mock import Mock
from pyddq.core import Check
from pyddq.utils import iterableAsScalaList


class ConstraintTest(unittest.TestCase):
    COLUMN_NAME = "column name"

    def setUp(self):
        self.check = Check(Mock())
        self.jvmCheck = self.check.jvmCheck

    def testHasUniqueKey(self):
        columnNames = ["a", "b"]
        self.check.hasUniqueKey(self.COLUMN_NAME, columnNames)
        self.jvmCheck.hasUniqueKey.assert_called_with(
            self.COLUMN_NAME,
            iterableAsScalaList(self.check._jvm, columnNames)
        )

    def testIsNeverNull(self):
        self.check.isNeverNull(self.COLUMN_NAME)
        self.jvmCheck.isNeverNull.assert_called_with(self.COLUMN_NAME)

    def testIsAlwaysNull(self):
        self.check.isAlwaysNull(self.COLUMN_NAME)
        self.jvmCheck.isAlwaysNull.assert_called_with(self.COLUMN_NAME)

    def testIsConvertibleTo(self):
        targetType = Mock()
        targetType.json = Mock(return_value="json value")
        jvmType = Mock()
        self.check._jvm.org.apache.spark.sql.types.DataType.fromJson = Mock(
            return_value=jvmType
        )

        self.check.isConvertibleTo(self.COLUMN_NAME, targetType)

        targetType.json.assert_called()
        self.check._jvm.org.apache.spark.sql.types.DataType.fromJson.\
            assert_called_with("json value")
        self.jvmCheck.isConvertibleTo.assert_called_with(
            self.COLUMN_NAME,
            jvmType
        )

    def testIsFormattedAsDate(self):
        dateFormat = "yyyy-MM-dd HH:mm:ss"
        jvmDateFormat = Mock()
        self.check._jvm.java.text.SimpleDateFormat = Mock(
            return_value=jvmDateFormat
        )
        self.check.isFormattedAsDate(self.COLUMN_NAME, dateFormat)
        self.jvmCheck.isFormattedAsDate.assert_called_with(self.COLUMN_NAME,
                                                           jvmDateFormat)

    def testIsAnyOf(self):
        allowed = ("a", "b", "c")
        jvmAllowed = Mock()
        self.check._jvm.scala.collection.JavaConversions.\
            iterableAsScalaIterable().toSet = Mock(
                return_value=jvmAllowed
        )
        self.check.isAnyOf(self.COLUMN_NAME, allowed)
        self.jvmCheck.isAnyOf.assert_called_with(self.COLUMN_NAME, jvmAllowed)

    def testIsMatchingRegex(self):
        regex = "^regex$"
        self.check.isMatchingRegex(self.COLUMN_NAME, regex)
        self.jvmCheck.isMatchingRegex.assertcalled_with(self.COLUMN_NAME, regex)

    def testHasFunctionalDepdendency(self):
        determinantSet = ["column1", "column2"]
        dependentSet = ["column3", "column4"]

        jvmDeterminantSet = Mock()
        jvmDependentSet = Mock()
        self.check._jvm.scala.collection.JavaConversions.\
            iterableAsScalaIterable().toList = Mock(
                side_effect=[jvmDeterminantSet, jvmDependentSet]
        )

        self.check.hasFunctionalDependency(determinantSet, dependentSet)
        self.jvmCheck.hasFunctionalDependency.assert_called_with(
            jvmDeterminantSet, jvmDependentSet
        )

    def testHasForeignKey(self):
        keyMap1 = ("_1", "_1")
        keyMap2 = ("_1", "_2")

        ref = Mock()
        jvmKeyMap1 = Mock()
        jvmKeyMap2 = Mock()

        self.check._jvm.scala.Tuple2 = Mock(
            side_effect=[jvmKeyMap1, jvmKeyMap2]
        )
        self.check._jvm.scala.collection.JavaConversions.\
            iterableAsScalaIterable().toList = Mock(
                return_value=[jvmKeyMap2]
        )
        self.check.hasForeignKey(ref, keyMap1, keyMap2)
        self.jvmCheck.hasForeignKey.assert_called_with(
            ref._jdf, jvmKeyMap1, [jvmKeyMap2]
        )

    def testIsJoinableWith(self):
        keyMap1 = ("_1", "_1")
        keyMap2 = ("_1", "_2")

        ref = Mock()
        jvmKeyMap1 = Mock()
        jvmKeyMap2 = Mock()

        self.check._jvm.scala.Tuple2 = Mock(
            side_effect=[jvmKeyMap1, jvmKeyMap2]
        )
        self.check._jvm.scala.collection.JavaConversions.\
            iterableAsScalaIterable().toList = Mock(
                return_value=[jvmKeyMap2]
        )
        self.check.isJoinableWith(ref, keyMap1, keyMap2)
        self.jvmCheck.isJoinableWith.assert_called_with(
            ref._jdf, jvmKeyMap1, [jvmKeyMap2]
        )


if __name__ == '__main__':
    unittest.main()
