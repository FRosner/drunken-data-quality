resolvers += "Spark Package Main Repo" at "https://dl.bintray.com/spark-packages/maven"

resolvers += "Typesafe Repository" at "https://repo.typesafe.com/typesafe/releases/"

addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.5")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.3.5")
