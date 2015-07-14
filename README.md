## Drunken Data Quality (DDQ) [![Build Status](https://travis-ci.org/FRosner/drunken-data-quality.svg?branch=master)](https://travis-ci.org/FRosner/drunken-data-quality) [![Latest Release](https://img.shields.io/github/tag/FRosner/drunken-data-quality.svg?label=JitPack)](https://jitpack.io/#FRosner/drunken-data-quality)

### Description

DDQ is a small library for checking constraints on Spark data structures. It can be used to assure a certain data quality, especially when continuous imports happen.

### Getting DDQ

In order to use DDQ, you can add it as a dependency to your project using [JitPack.io](https://jitpack.io/#FRosner/drunken-data-quality). If you are not using any of the supported dependency management systems, feel free to download one of the compiled artifacts in the [release section](https://github.com/FRosner/drunken-data-quality/releases). Alternatively you may of course also build from source.

### Using DDQ

```scala
import de.frosner.ddq._

val customers = sqlContext.table("customers")
Check(customers).hasNumRowsEqualTo(100000).hasKey("customer_id").satisfies("customer_age > 0").run()
```


