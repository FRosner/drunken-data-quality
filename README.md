# Drunken Data Quality (DDQ) <img src="https://raw.githubusercontent.com/FRosner/drunken-data-quality/master/logo/DDQ_small.png" alt="Logo" height="25">

[![Join the chat at https://gitter.im/FRosner/drunken-data-quality](https://badges.gitter.im/FRosner/drunken-data-quality.svg)](https://gitter.im/FRosner/drunken-data-quality?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) [![Build Status](https://travis-ci.org/FRosner/drunken-data-quality.svg?branch=master)](https://travis-ci.org/FRosner/drunken-data-quality) [![Codacy Badge](https://api.codacy.com/project/badge/grade/b738700bad0a4b6da14e06c0dd508a21)](https://www.codacy.com/app/frank_7/drunken-data-quality) [![codecov.io](https://codecov.io/github/FRosner/drunken-data-quality/coverage.svg?branch=master)](https://codecov.io/github/FRosner/drunken-data-quality?branch=master)

## Description

DDQ is a small library for checking constraints on Spark data structures. It can be used to assure a certain data quality, especially when continuous imports happen.

## Getting DDQ

### Spark Package

DDQ is available as a [spark package](http://spark-packages.org/package/FRosner/drunken-data-quality). You can add it to your spark-shell, spark-submit or pyspark using the `--packages` command line option:

```sh
spark-shell --packages FRosner:drunken-data-quality:4.1.0-s_2.11
```

### Python API

DDQ also comes with a Python API. It is available via the Python Package Index, so you have to install it once using `pip`:

```
pip install pyddq==4.1.0
```

### Project Dependency [![Latest Release](https://img.shields.io/github/tag/FRosner/drunken-data-quality.svg?label=JitPack)](https://jitpack.io/#FRosner/drunken-data-quality)

In order to use DDQ in your project, you can add it as a library dependency. This can be done through the [SBT spark package plugin](https://github.com/databricks/sbt-spark-package), or you can add it using [JitPack.io](https://jitpack.io/#FRosner/drunken-data-quality).

Keep in mind that you might need to add additional resolvers as DDQ has some external dependencies starting from version 4.1.0:

```
resolvers += "lightshed-maven" at "http://dl.bintray.com/content/lightshed/maven"
```

If neither of the above-mentioned ways work for you, feel free to download one of the compiled artifacts in the [release section](https://github.com/FRosner/drunken-data-quality/releases). Alternatively you may of course also build from source.

## Using DDQ

### Getting Started

Create two example tables to play with (or use your existing ones).

```scala
case class Customer(id: Int, name: String)
case class Contract(id: Int, customerId: Int, duration: Int)

val customers = spark.createDataFrame(List(
  Customer(0, "Frank"),
  Customer(1, "Alex"),
  Customer(2, "Slavo")
))

val contracts = spark.createDataFrame(List(
  Contract(0, 0, 5),
  Contract(1, 0, 10),
  Contract(0, 1, 6)
))
```

Run some checks and see the results on the console.

```scala
import de.frosner.ddq.core._

Check(customers)
  .hasNumRows(_ >= 3)
  .hasUniqueKey("id")
  .run()

Check(contracts)
  .hasNumRows(_ > 0)
  .hasUniqueKey("id", "customerId")
  .satisfies("duration > 0")
  .hasForeignKey(customers, "customerId" -> "id")
  .run()
```

### Custom Reporters

By default the check result will be printed to stdout using ANSI escape codes to highlight the output. To have a report in another format, you can specify one or more [custom reporters](https://github.com/FRosner/drunken-data-quality/wiki).

```scala
import de.frosner.ddq.reporters.MarkdownReporter

Check(customers)
  .hasNumRows(_ >= 3)
  .hasUniqueKey("id")
  .run(MarkdownReporter(System.err))
```

### Running multiple checks

You can use a runner to generate reports for multiple checks at once. It will execute all the checks and report the results to the specified reporters.

```scala
import de.frosner.ddq.reporters.ConsoleReporter
import java.io.{PrintStream, File}

val check1 = Check(customers)
  .hasNumRows(_ >= 3)
  .hasUniqueKey("id")

val check2 = Check(contracts)
  .hasNumRows(_ > 0)
  .hasUniqueKey("id", "customerId")
  .satisfies("duration > 0")
  .hasForeignKey(customers, "customerId" -> "id")

val consoleReporter = new ConsoleReporter(System.out)
val markdownMd = new PrintStream(new File("report.md"))
val markdownReporter = new MarkdownReporter(markdownMd)

Runner.run(Seq(check1, check2), Seq(consoleReporter, markdownReporter))

markdownMd.close()
```

### Unit Tests

You can also use DDQ to write automated quality tests for your data. After running a check or a series of checks, you can inspect the results programmatically.

```scala
def allConstraintsSatisfied(checkResult: CheckResult): Boolean =
  checkResult.constraintResults.map {
    case (constraint, ConstraintSuccess(_)) => true
    case (constraint, ConstraintFailure(_)) => false
  }.reduce(_ && _)

val results = Runner.run(Seq(check1, check2), Seq.empty)
assert(allConstraintsSatisfied(results(check1)))
assert(allConstraintsSatisfied(results(check2)))
```

If you want to fail the data load if the number of rows and the unique key constraints are not satisfied, but the duration constraint can be violated, you can write individual assertions for each constraint result.

```scala
val numRowsConstraint = Check.hasNumRows(_ >= 3)
val uniqueKeyConstraint = Check.hasUniqueKey("id", "customerId")
val durationConstraint = Check.satisfies("duration > 0")

val check = Check(contracts)
  .addConstraint(numRowsConstraint)
  .addConstraint(uniqueKeyConstraint)
  .addConstraint(durationConstraint)

val results = Runner.run(Seq(check), Seq.empty)
val constraintResults = results(check).constraintResults
assert(constraintResults(numRowsConstraint).isInstanceOf[ConstraintSuccess])
assert(constraintResults(uniqueKeyConstraint).isInstanceOf[ConstraintSuccess])
```

### Python API

In order to use the Python API, you have to start PySpark with the DDQ jar added. Unfortunately, using the `--packages` way is [not working in Spark < 2.0](https://issues.apache.org/jira/browse/SPARK-5185).

```
pyspark --driver-class-path drunken-data-quality_2.11-x.y.z.jar
```

Then you can create a dummy dataframe and run a few checks.

```python
from pyddq.core import Check

df = spark.createDataFrame([(1, "a"), (1, None), (3, "c")])
check = Check(df)
check.hasUniqueKey("_1", "_2").isNeverNull("_1").run()
```

Just as the Scala version of DDQ, PyDDQ supports multiple reporters.
In order to facilitate them, you can use `pyddq.streams`, which wraps the Java streams.

```python
from pyddq.reporters import MarkdownReporter, ConsoleReporter
from pyddq.streams import FileOutputStream, ByteArrayOutputStream
import sys

# send the report in a console-friendly format the standard output
# and in markdown format to the bytearray
stdout_stream = FileOutputStream(sys.stdout)
bytearray_stream = ByteArrayOutputStream()

Check(df)\
    .hasUniqueKey("_1", "_2")\
    .isNeverNull("_1")\
    .run([MarkdownReporter(bytearray_stream), ConsoleReporter(stdout_stream)])

# print markdown report
print bytearray_stream.get_output()
```

## Spark Version Compatibility

Although we try to maintain as much compatibility between all available Spark versions we cannot guarantee that everything works smoothly for every possible combination of DDQ and Spark versions. The following matrix shows you what version of DDQ is built and tested against what version of Spark.

DDQ Version | Spark Version
--- | ---
4.x | 2.0.x
3.x | 1.6.x
2.x | 1.3.x
1.x | 1.3.x

## Documentation

For a comprehensive list of available constraints, please refer to the [Wiki](https://github.com/FRosner/drunken-data-quality/wiki).

## Authors

- [Frank Rosner](https://github.com/FRosner) (Core, Reporters, Scala API)
- [Aleksandr Sorokoumov](https://github.com/Gerrrr) (Python API)

Thanks also to everyone who submitted pull requests, bug reports and feature requests.

## License

This project is licensed under the Apache License Version 2.0. For details please see the file called LICENSE.
