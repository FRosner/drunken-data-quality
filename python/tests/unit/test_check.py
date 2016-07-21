import unittest
from mock import Mock
from pyddq.core import Check


class ConstructorTest(unittest.TestCase):
    def test_default_args(self):
        df = Mock()
        check = Check(df)
        ddq_check = check._jvm.de.frosner.ddq.core.Check
        ddq_check.assert_called_with(
            df._jdf,
            getattr(ddq_check, "apply$default$2")(),
            getattr(ddq_check, "apply$default$3")(),
            getattr(ddq_check, "apply$default$4")(),
            getattr(ddq_check, "apply$default$5")(),
        )

    def test_passed_args(self):
        df = Mock()
        display_name = Mock()
        cache_method = Mock()
        id = Mock()

        df._sc._jvm.scala.Some.apply = Mock(
            side_effect=["Some(displayName)", "Some(cacheMethod)"]
        )
        check = Check(df, display_name, cache_method, id)
        ddq_check = check._jvm.de.frosner.ddq.core.Check

        ddq_check.assert_called_with(
            df._jdf,
            "Some(displayName)",
            "Some(cacheMethod)",
            getattr(ddq_check, "apply$default$4")(),
            id
        )

if __name__ == '__main__':
    unittest.main()
