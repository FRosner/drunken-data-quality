import unittest
from uuid import UUID, uuid4

from pyspark import SparkContext, StorageLevel
from pyspark.sql import SQLContext

from pyddq.core import Check

class ConstructorTest(unittest.TestCase):
    def setUp(self):
        self.sc = SparkContext()
        self.sql = SQLContext(self.sc)
        self.df = self.sql.createDataFrame([(1, "a"), (1, None), (3, "c")])

    def test_default_args(self):
        check = Check(self.df)
        self.assertEqual(check.name, "DataFrame[_1: bigint, _2: string]")
        self.assertEqual(check.cacheMethod, None)
        try:
            UUID(check.id, version=4)
        except ValueError:
            raise self.fail("id is not a correct uuid4")

        self.assertEqual(
            check.jvmCheck.getClass().toString(),
            "class de.frosner.ddq.core.Check"
        )

    def test_passed_args(self):
        display_name = "display name"
        id = "id"
        cache_method = StorageLevel.DISK_ONLY
        check = Check(self.df, display_name, cache_method, id)

        # check wrapper
        self.assertEqual(check.name, display_name)
        self.assertEqual(check.id, id)
        self.assertEqual(check.cacheMethod, cache_method)

        # check jvm check
        self.assertEqual(
            check.jvmCheck.getClass().toString(),
            "class de.frosner.ddq.core.Check"
        )
        self.assertEqual(check.jvmCheck.name(), check.name)
        self.assertEqual(check.jvmCheck.id(), check.id)
        jvm_cache_method = check.jvmCheck.cacheMethod().get()
        self.assertEqual(
            jvm_cache_method.useDisk(),
            check.cacheMethod.useDisk
        )
        self.assertEqual(
            jvm_cache_method.useMemory(),
            check.cacheMethod.useMemory
        )
        self.assertEqual(
            jvm_cache_method.useOffHeap(),
            check.cacheMethod.useOffHeap
        )
        self.assertEqual(
            jvm_cache_method.deserialized(),
            check.cacheMethod.deserialized
        )
        self.assertEqual(
            jvm_cache_method.replication(),
            check.cacheMethod.replication
        )

    def tearDown(self):
        self.sc.stop()

if __name__ == '__main__':
    unittest.main()
