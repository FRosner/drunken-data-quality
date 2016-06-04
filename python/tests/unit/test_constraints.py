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


if __name__ == '__main__':
    unittest.main()
