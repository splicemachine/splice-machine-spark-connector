name := "splice-machine-spark-connector"

version := "0.1"

scalaVersion := "2.11.12"

// FIXME Spark 2.3.2 seems the latest supported by splice machine
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2" % Provided
dependencyOverrides += "org.apache.spark" %% "spark-sql" % "2.3.2"

val scalatestVer = "3.0.7"
libraryDependencies += "org.scalactic" %% "scalactic" % scalatestVer force()
libraryDependencies += "org.scalatest" %% "scalatest" % scalatestVer % Test force()
// FIXME Somehow dependencies include scalatest 2.2.6 in Runtime scope!
dependencyOverrides += "org.scalatest" %% "scalatest" % scalatestVer

resolvers +=
  "splicemachine-public" at "http://repository.splicemachine.com/nexus/content/groups/public"
resolvers +=
  "mapr-public" at "http://repository.mapr.com/maven/"

// FIXME classifiers do not work with coursier
// see https://github.com/sbt/sbt/issues/285
val envClassifier = "mapr6.1.0"
val hbaseVersion = "1.1.8-mapr-1901"
val spliceVersion = "latest.version"
libraryDependencies += "com.splicemachine" % "splice_spark" % spliceVersion classifier envClassifier

// com.fasterxml.jackson.databind.JsonMappingException: Incompatible Jackson version: 2.9.2
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.8" force()

// FIXME The following dependencies fail in sbt due to envClassifier property not being resolved
// The pattern is to exclude them first and add them explicitly right after

// FIXME hbase_storage should actually be dependency of splice_spark
// java.lang.NoClassDefFoundError: com/splicemachine/access/HConfiguration
// https://stackoverflow.com/a/46763742/1305344
lazy val mavenProps = settingKey[Unit]("workaround for Maven properties")
mavenProps := {
  sys.props("envClassifier") = envClassifier
  sys.props("hbase.version") = hbaseVersion
  ()
}

libraryDependencies +=
  "com.splicemachine" % "hbase_storage" % spliceVersion classifier envClassifier force()
dependencyOverrides += "com.splicemachine" % "hbase_storage" % spliceVersion classifier envClassifier

// FIXME hbase_sql should actually be dependency of splice_spark
//  ClassNotFoundException: com.splicemachine.derby.impl.SpliceSpark
libraryDependencies +=
  "com.splicemachine" % "hbase_sql" % spliceVersion classifier envClassifier

// default-jar execution in scala_util/pom.xml changes the classifier
// And build.sbt sets envClassifier that is different from expected value
// FIXME How to include scala_util?! The following does not work
excludeDependencies ++= Seq(
  ExclusionRule("com.splicemachine", "scala_util")
)

val sparkVersionMapr610 = s"$envClassifier-2.3.2.0-mapr-1901"
libraryDependencies +=
  "com.splicemachine" % "scala_util" % spliceVersion classifier sparkVersionMapr610 force()
dependencyOverrides += "com.splicemachine" % "scala_util" % spliceVersion classifier sparkVersionMapr610

libraryDependencies +=
  "com.splicemachine" % "hbase_pipeline" % spliceVersion classifier envClassifier

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.3" force()
libraryDependencies += "org.apache.hbase" % "hbase-common" % hbaseVersion