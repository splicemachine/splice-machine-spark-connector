name := "splice-machine-spark-connector"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0" % Provided

val scalatestVer = "3.0.6"
libraryDependencies += "org.scalactic" %% "scalactic" % scalatestVer
libraryDependencies += "org.scalatest" %% "scalatest" % scalatestVer % Test
