# Drunken Data Quality (DDQ) [![Build Status](https://travis-ci.org/FRosner/drunken-data-quality.svg?branch=master)](https://travis-ci.org/FRosner/drunken-data-quality) [![Codacy Badge](https://api.codacy.com/project/badge/grade/b738700bad0a4b6da14e06c0dd508a21)](https://www.codacy.com/app/frank_7/drunken-data-quality) [![codecov.io](https://codecov.io/github/FRosner/drunken-data-quality/coverage.svg?branch=master)](https://codecov.io/github/FRosner/drunken-data-quality?branch=master)

## Description

DDQ is a small library for checking constraints on Spark data structures. It can be used to assure a certain data quality, especially when continuous imports happen.

## Getting DDQ [![Latest Release](https://img.shields.io/github/tag/FRosner/drunken-data-quality.svg?label=JitPack)](https://jitpack.io/#FRosner/drunken-data-quality)

In order to use DDQ, you can add it as a dependency to your project using [JitPack.io](https://jitpack.io/#FRosner/drunken-data-quality). Just add it to your `build.sbt` like this:

```scala
resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies += "com.github.FRosner" % "drunken-data-quality" % "x.y.z"
```

If you are not using any of the dependency management systems supported by JitPack, feel free to download one of the compiled artifacts in the [release section](https://github.com/FRosner/drunken-data-quality/releases). Alternatively you may of course also build from source.

## Using DDQ

### Getting Started

Create two example tables to play with (or use your existing ones).

```scala
import org.apache.spark.sql._
val sqlContext = new SQLContext(sc)
import sqlContext.implicits._

case class Customer(id: Int, name: String)
case class Contract(id: Int, customerId: Int, duration: Int)

val customers = sc.parallelize(List(
  Customer(0, "Frank"),
  Customer(1, "Alex"),
  Customer(2, "Slavo")
)).toDF

val contracts = sc.parallelize(List(
  Contract(0, 0, 5),
  Contract(1, 0, 10),
  Contract(0, 1, 6)
)).toDF
```

Run some checks and see the results on the console.

```scala
import de.frosner.ddq.core._

Check(customers)
  .hasNumRowsEqualTo(3)
  .hasUniqueKey("id")
  .run()

Check(contracts)
  .hasNumRowsEqualTo(3)
  .hasUniqueKey("id", "customerId")
  .satisfies("duration > 0")
  .hasForeignKey(customers, "customerId" -> "id")
  .run()
```

### Custom Reporters

By default the check result will be printed to stdout using ANSI escape codes to highlight the output. To have a report in another format, you can specify one a more custom reporters.

```scala
import de.frosner.ddq.reporters.MarkdownReporter

Check(customers)
  .hasNumRowsEqualTo(3)
  .hasUniqueKey("id")
  .run(MarkdownReporter(System.err))
```

### Running multiple checks

You can use a runner to generate reports for multiple checks at once. It will execute all the checks and report the results to the specified reporters.

```scala
import de.frosner.ddq.reporters.ConsoleReporter
import java.io.{PrintStream, File}

val check1 = Check(customers)
  .hasNumRowsEqualTo(3)
  .hasUniqueKey("id")

val check2 = Check(contracts)
  .hasNumRowsEqualTo(3)
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
val numRowsConstraint = Check.hasNumRowsEqualTo(3)
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

## Authors

- [Frank Rosner](https://github.com/FRosner) (Creator)
- [Aleksandr Sorokoumov](https://github.com/Gerrrr) (Contributor)
- [Slavo N.](https://github.com/mfsny) (Contributor)

## License

This project is licensed under the Apache License Version 2.0. For details please see the file called LICENSE.
