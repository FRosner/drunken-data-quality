import unittest
from uuid import UUID, uuid4

from pyspark import SparkContext, StorageLevel
from pyspark.sql import SQLContext

from pyddq.core import Check

class ConstructorTest(unittest.TestCase):
    def setUp(self):
        self.sc = SparkContext()
        self.sqlContext = SQLContext(self.sc)
        self.df = self.sqlContext.createDataFrame([(1, "a"), (1, None), (3, "c")])

    def testDefaultArgs(self):
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

    def testPassedArgs(self):
        displayName = "display name"
        id = "id"
        cacheMethod = StorageLevel.DISK_ONLY
        check = Check(self.df, displayName, cacheMethod, id)

        # check wrapper
        self.assertEqual(check.name, displayName)
        self.assertEqual(check.id, id)
        self.assertEqual(check.cacheMethod, cacheMethod)

        # check jvm check
        self.assertEqual(
            check.jvmCheck.getClass().toString(),
            "class de.frosner.ddq.core.Check"
        )
        self.assertEqual(check.jvmCheck.name(), check.name)
        self.assertEqual(check.jvmCheck.id(), check.id)
        jvmCacheMethod = check.jvmCheck.cacheMethod().get()
        self.assertEqual(
            jvmCacheMethod.useDisk(),
            check.cacheMethod.useDisk
        )
        self.assertEqual(
            jvmCacheMethod.useMemory(),
            check.cacheMethod.useMemory
        )
        self.assertEqual(
            jvmCacheMethod.useOffHeap(),
            check.cacheMethod.useOffHeap
        )
        self.assertEqual(
            jvmCacheMethod.deserialized(),
            check.cacheMethod.deserialized
        )
        self.assertEqual(
            jvmCacheMethod.replication(),
            check.cacheMethod.replication
        )




    def tearDown(self):
        self.sc.stop()

if __name__ == '__main__':
    unittest.main()
