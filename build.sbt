organization  := "de.frosner"

version       := "5.1.0-SNAPSHOT"

name          := "drunken-data-quality"

scalaVersion  := "2.11.11"

sparkVersion := "2.2.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided"

libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion.value % "provided"

libraryDependencies += "org.mockito" % "mockito-all" % "1.8.4" % "test"

resolvers += "lightshed-maven" at "http://dl.bintray.com/content/lightshed/maven"

libraryDependencies += "ch.lightshed" %% "courier" % "0.1.4"

libraryDependencies += "org.jvnet.mock-javamail" % "mock-javamail" % "1.9" % "test"

spName := "FRosner/drunken-data-quality"

spAppendScalaVersion := true

spIgnoreProvided := true

sparkComponents ++= Seq("sql", "hive")

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

fork := true

javaOptions += "-Xmx2G"

lazy val pythonItAssembly = taskKey[Unit]("python-it-assembly")

pythonItAssembly <<= assembly map { (asm) => s"cp ${asm.getAbsolutePath()} python/drunken-data-quality.jar" ! }
