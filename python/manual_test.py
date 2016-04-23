from pyddq.core import Check
from pyddq.reporters import MarkdownReporter
import sys

rdd = sc.parallelize([(1, "a"), (2, "b"), (3, "c")])
df = sqlContext.createDataFrame(rdd)

md_reporter_1 = MarkdownReporter(open("report.md", "w+"))
md_reporter_2 = MarkdownReporter(sys.stdout)


check = Check(df)
check.isNeverNull("_1").run([md_reporter_1, md_reporter_2])
