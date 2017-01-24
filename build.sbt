organization  := "de.frosner"

version       := "4.0.0-SNAPSHOT"

name          := "drunken-data-quality"

scalaVersion  := "2.11.8"

crossScalaVersions := Seq("2.10.6", "2.11.8")

sparkVersion := "2.0.2"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided"

libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion.value % "provided"

libraryDependencies += "org.mockito" % "mockito-all" % "1.8.4" % "test"

spName := "FRosner/drunken-data-quality"

spAppendScalaVersion := true

spIgnoreProvided := true

sparkComponents ++= Seq("sql", "hive")

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

fork := true

javaOptions += "-Xmx2G"

lazy val pythonItAssembly = taskKey[Unit]("python-it-assembly")

pythonItAssembly <<= assembly map { (asm) => s"cp ${asm.getAbsolutePath()} python/drunken-data-quality.jar" ! }