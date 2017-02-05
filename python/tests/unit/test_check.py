import unittest
from mock import Mock, call
from pyddq.core import Check
from pyddq.exceptions import JavaClassNotFoundException
from utils import get_df

class ConstructorTest(unittest.TestCase):
    def setUp(self):
        self.df = get_df()

    def test_default_args(self):
        check = Check(self.df)
        ddq_core = check._jvm.de.frosner.ddq.core
        ddq_check = ddq_core.Check

        ddq_core.assert_has_calls([
            call.Check(
                self.df._jdf,
                getattr(ddq_check, "apply$default$2")(),
                getattr(ddq_check, "apply$default$3")(),
                getattr(ddq_check, "apply$default$4")(),
                getattr(ddq_check, "apply$default$5")()
            )
        ])

    def test_passed_args(self):
        display_name = Mock()
        cache_method = Mock()
        id = Mock()

        self.df._sc._jvm.scala.Some.apply = Mock(
            side_effect=["Some(displayName)", "Some(cacheMethod)"]
        )
        check = Check(self.df, display_name, cache_method, id)
        ddq_core = check._jvm.de.frosner.ddq.core
        ddq_check = check._jvm.de.frosner.ddq.core.Check

        ddq_core.assert_has_calls([
            call.Check(
                self.df._jdf,
                "Some(displayName)",
                "Some(cacheMethod)",
                getattr(ddq_check, "apply$default$4")(),
                id
            )
        ])

    def test_ddq_jar(self):
        self.assertRaises(JavaClassNotFoundException, Check, Mock())

if __name__ == '__main__':
    unittest.main()
