import unittest
from mock import Mock
from pyddq.core import Check


class ConstructorTest(unittest.TestCase):
    def testDefaultArgs(self):
        dataFrame = Mock()
        check = Check(dataFrame)
        ddqCheck = check._jvm.de.frosner.ddq.core.Check
        ddqCheck.assert_called_with(
            dataFrame._jdf,
            getattr(ddqCheck, "apply$default$2")(),
            getattr(ddqCheck, "apply$default$3")(),
            getattr(ddqCheck, "apply$default$4")(),
            getattr(ddqCheck, "apply$default$5")(),
        )

    def testPassedArgs(self):
        dataFrame = Mock()
        displayName = Mock()
        cacheMethod = Mock()
        id = Mock()

        dataFrame._sc._jvm.scala.Some.apply = Mock(
            side_effect=["Some(displayName)", "Some(cacheMethod)"]
        )
        check = Check(dataFrame, displayName, cacheMethod, id)
        ddqCheck = check._jvm.de.frosner.ddq.core.Check

        ddqCheck.assert_called_with(
            dataFrame._jdf,
            "Some(displayName)",
            "Some(cacheMethod)",
            getattr(ddqCheck, "apply$default$4")(),
            id
        )
