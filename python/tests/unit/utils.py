from mock import Mock
from py4j.java_gateway import JavaClass

def get_df():
    df = Mock()
    df._sc._jvm.de.frosner.ddq.core.Check = Mock(spec=JavaClass)
    setattr(df._sc._jvm.de.frosner.ddq.core.Check, "apply$default$2", Mock())
    setattr(df._sc._jvm.de.frosner.ddq.core.Check, "apply$default$3", Mock())
    setattr(df._sc._jvm.de.frosner.ddq.core.Check, "apply$default$4", Mock())
    setattr(df._sc._jvm.de.frosner.ddq.core.Check, "apply$default$5", Mock())
    return df
