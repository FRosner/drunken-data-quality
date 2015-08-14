organization  := "de.frosner"

version       := "1.3.0-SNAPSHOT"

name          := "drunken-data-quality"

scalaVersion  := "2.10.5"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.3.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.3.0" % "provided"

libraryDependencies += "org.scalamock" %% "scalamock-scalatest-support" % "3.2.1" % "test"

libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.10"

fork := true

javaOptions += "-Xmx2G"

javaOptions += "-XX:MaxPermSize=128m"
