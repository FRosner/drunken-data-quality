import unittest
from mock import Mock
from py4j.java_gateway import JavaClass
from pyddq.core import Check
from utils import get_df


class ConstraintTest(unittest.TestCase):
    COLUMN_NAME = "column name"

    def setUp(self):
        df = get_df()
        self.check = Check(df)
        self.jvmCheck = self.check.jvmCheck

    def test_hasUniqueKey(self):
        column_names = ["a", "b"]
        jvm_column_names = Mock()
        self.check._jvm.scala.collection.JavaConversions.\
            iterableAsScalaIterable().toList = Mock(
                return_value=jvm_column_names
        )
        self.check.hasUniqueKey(self.COLUMN_NAME, column_names)
        self.jvmCheck.hasUniqueKey.assert_called_with(
            self.COLUMN_NAME, jvm_column_names
        )

    def test_isNeverNull(self):
        self.check.isNeverNull(self.COLUMN_NAME)
        self.jvmCheck.isNeverNull.assert_called_with(self.COLUMN_NAME)

    def test_isAlwaysNull(self):
        self.check.isAlwaysNull(self.COLUMN_NAME)
        self.jvmCheck.isAlwaysNull.assert_called_with(self.COLUMN_NAME)

    def test_isConvertibleTo(self):
        target_type = Mock()
        target_type.json = Mock(return_value="json value")
        jvm_type = Mock()
        self.check._jvm.org.apache.spark.sql.types.DataType.fromJson = Mock(
            return_value=jvm_type
        )

        self.check.isConvertibleTo(self.COLUMN_NAME, target_type)

        target_type.json.assert_called()
        self.check._jvm.org.apache.spark.sql.types.DataType.fromJson.\
            assert_called_with("json value")
        self.jvmCheck.isConvertibleTo.assert_called_with(
            self.COLUMN_NAME,
            jvm_type
        )

    def test_isFormattedAsDate(self):
        date_format = "yyyy-MM-dd HH:mm:ss"
        self.check.isFormattedAsDate(self.COLUMN_NAME, date_format)
        self.jvmCheck.isFormattedAsDate.assert_called_with(self.COLUMN_NAME,
                                                           date_format)

    def test_isAnyOf(self):
        allowed = ("a", "b", "c")
        jvm_allowed = Mock()
        self.check._jvm.scala.collection.JavaConversions.\
            iterableAsScalaIterable().toSet = Mock(
                return_value=jvm_allowed
        )
        self.check.isAnyOf(self.COLUMN_NAME, allowed)
        self.jvmCheck.isAnyOf.assert_called_with(self.COLUMN_NAME, jvm_allowed)

    def test_isMatchingRegex(self):
        regex = "^regex$"
        self.check.isMatchingRegex(self.COLUMN_NAME, regex)
        self.jvmCheck.isMatchingRegex.assert_called_with(self.COLUMN_NAME, regex)

    def test_hasFunctionalDepdendency(self):
        determinant_set = ["column1", "column2"]
        dependent_set = ["column3", "column4"]

        jvm_determinant_set = Mock()
        jvm_dependent_set = Mock()
        self.check._jvm.scala.collection.JavaConversions.\
            iterableAsScalaIterable().toList = Mock(
                side_effect=[jvm_determinant_set, jvm_dependent_set]
        )

        self.check.hasFunctionalDependency(determinant_set, dependent_set)
        self.jvmCheck.hasFunctionalDependency.assert_called_with(
            jvm_determinant_set, jvm_dependent_set
        )

    def test_hasForeignKey(self):
        key_map1 = ("_1", "_1")
        key_map2 = ("_1", "_2")

        ref = Mock()
        jvm_key_map1 = Mock()
        jvm_key_map2 = Mock()

        self.check._jvm.scala.Tuple2 = Mock(
            side_effect=[jvm_key_map1, jvm_key_map2]
        )
        self.check._jvm.scala.collection.JavaConversions.\
            iterableAsScalaIterable().toList = Mock(
                return_value=[jvm_key_map2]
        )
        self.check.hasForeignKey(ref, key_map1, key_map2)
        self.jvmCheck.hasForeignKey.assert_called_with(
            ref._jdf, jvm_key_map1, [jvm_key_map2]
        )

    def test_isJoinableWith(self):
        key_map1 = ("_1", "_1")
        key_map2 = ("_1", "_2")

        ref = Mock()
        jvm_key_map1 = Mock()
        jvm_key_map2 = Mock()

        self.check._jvm.scala.Tuple2 = Mock(
            side_effect=[jvm_key_map1, jvm_key_map2]
        )
        self.check._jvm.scala.collection.JavaConversions.\
            iterableAsScalaIterable().toList = Mock(
                return_value=[jvm_key_map2]
        )
        self.check.isJoinableWith(ref, key_map1, key_map2)
        self.jvmCheck.isJoinableWith.assert_called_with(
            ref._jdf, jvm_key_map1, [jvm_key_map2]
        )

    def test_satisfies(self):
        constraint = "_1 > 10"
        self.check.satisfies(constraint)
        self.jvmCheck.satisfies.assert_called_with(constraint)

    def test_isEqualTo(self):
        df2 = Mock()
        self.check.isEqualTo(df2)
        self.jvmCheck.isEqualTo.assert_called_with(df2._jdf)

if __name__ == '__main__':
    unittest.main()
