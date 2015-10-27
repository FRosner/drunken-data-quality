# Drunken Data Quality (DDQ) [![Build Status](https://travis-ci.org/FRosner/drunken-data-quality.svg?branch=master)](https://travis-ci.org/FRosner/drunken-data-quality) [![Coverage Status](https://coveralls.io/repos/FRosner/drunken-data-quality/badge.svg?branch=master&service=github)](https://coveralls.io/github/FRosner/drunken-data-quality?branch=master)

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

```scala
import de.frosner.ddq.core._

val customers = sqlContext.table("customers")
val contracts = sqlContext.table("contracts")
Check(customers)
  .hasNumRowsEqualTo(100000)
  .isNeverNull("customer_id")
  .hasUniqueKey("customer_id")
  .satisfies("customer_age > 0")
  .isConvertibleToDate("customer_birthday", new SimpleDateFormat("yyyy-MM-dd"))
  .hasForeignKey(contracts, "customer_id" -> "contract_owner_id")
  .run()
```

### Reporters
By default the check result will be printed to stdout using ANSI escape codes to highlight the output. To have a report in another format, you can specify the reporter.
```scala
import de.frosner.ddq.core._
import de.frosner.ddq.reporters.{MarkdownReporter, ConsoleReporter}

val customers = sqlContext.table("customers")
val contracts = sqlContext.table("contracts")

val check = Check(customers)
  .hasNumRowsEqualTo(100000)
  .isNeverNull("customer_id")
  .hasUniqueKey("customer_id")
  .satisfies("customer_age > 0")
  .isConvertibleToDate("customer_birthday", new SimpleDateFormat("yyyy-MM-dd"))
  .hasForeignKey(contracts, "customer_id" -> "contract_owner_id")

check.run(new MarkdownReporter(System.out)) // will printed to stdout in Markdown format
// it also works with multiple Reporters
val consoleReporter = new ConsoleReporter(System.out)
val markdownReporter = new MarkdownReporter(new PrintStream(new File ("report.md"))
check.run(List(consoleReporter, markdownReporter))) // report to console and markdown file
```

### Runner
Runner is used to generate the reports for multiple checks with the same reporters.

```scala
val check1 = Check(customers)
  .hasNumRowsEqualTo(100000)
  .isNeverNull("customer_id")
  .hasUniqueKey("customer_id")

val check2 = Check(contracts)
  .hasNumRowsEqualTo(100000)
  .isNeverNull("contract_id")
  .hasUniqueKey("contract_id")

val consoleReporter = new ConsoleReporter(System.out)
val markdownReporter = new MarkdownReporter(new PrintStream(new File ("report.md"))

// will generate console and markdown report for both checks
Runner.run(Seq(check1, check2), Seq(consoleReporter, markdownReporter))
```

## Authors

- [Frank Rosner](https://github.com/FRosner) (Creator)
- [Aleksandr Sorokoumov](https://github.com/Gerrrr) (Contributor)
- [Slavo N.](https://github.com/mfsny) (Contributor)

## License

This project is licensed under the Apache License Version 2.0. For details please see the file called LICENSE.
