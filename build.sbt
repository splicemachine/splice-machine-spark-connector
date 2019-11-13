name := "splice-machine-spark-connector"

version := "0.1"

scalaVersion := "2.11.12"

val spliceVersion = "2.8.0.1935"

// https://github.com/sbt/sbt/issues/5046
ThisBuild / useCoursier := false

lazy val envClassifier = settingKey[String]("")
envClassifier := "cdh5.14.0"

lazy val hbaseVersion = settingKey[String]("")
hbaseVersion := s"1.2.0-${envClassifier.value}"

lazy val hadoopVersion = settingKey[String]("")
hadoopVersion := s"2.6.0-${envClassifier.value}"

// FIXME hbase_sql should actually be dependency of splice_spark
//  ClassNotFoundException: com.splicemachine.derby.impl.SpliceSpark

// FIXME hbase_storage should actually be dependency of splice_spark
// java.lang.NoClassDefFoundError: com/splicemachine/access/HConfiguration
// https://stackoverflow.com/a/46763742/1305344

val excludedDeps = Seq(
  ExclusionRule(organization = "org.xerial.snappy", name = "snappy-java"),
  ExclusionRule(organization = "tomcat", name = "jasper-compiler"),
  // FIXME Somehow 2.2.0 is pulled down
  ExclusionRule(organization = "org.apache.spark"),
  // Added later separately
  ExclusionRule(organization = "com.splicemachine", name = "scala_util")
)

libraryDependencies ++= Seq(
  "splice_spark",
  "hbase_sql",
  "hbase_storage",
  "hbase_pipeline",
  "spark_sql"
).map(spliceDep(_, envClassifier.value))

lazy val sparkVersion = settingKey[String]("")
sparkVersion := "2.2.0.cloudera2"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion.value % Provided

// Somehow it is necessary to add the dependency implicitly
// After it was excluded explicitly from all dependencies
lazy val scalaUtilClassifier = Def.setting {
  s"${envClassifier.value}-${sparkVersion.value}_${scalaBinaryVersion.value}"
}
libraryDependencies += spliceDep("scala_util", scalaUtilClassifier.value)

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion.value excludeAll(excludedDeps: _*)

// Required to ensure proper dependency
// (otherwise cdh5.12.0 version was resolved and used)
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion.value excludeAll (excludedDeps: _*)
libraryDependencies += "org.apache.hbase" % "hbase-server" % hbaseVersion.value excludeAll (excludedDeps: _*)
libraryDependencies += "org.apache.hbase" % "hbase-common" % hbaseVersion.value excludeAll (excludedDeps: _*)
libraryDependencies += "org.apache.hbase" % "hbase-hadoop-compat" % hbaseVersion.value excludeAll (excludedDeps: _*)

resolvers +=
  ("splicemachine-public" at "http://repository.splicemachine.com/nexus/content/groups/public")
    .withAllowInsecureProtocol(true)
resolvers +=
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
// For development only / local Splice SNAPSHOTs
resolvers += Resolver.mavenLocal

// com.fasterxml.jackson.databind.JsonMappingException: Incompatible Jackson version: 2.9.2
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.9" force() excludeAll (excludedDeps: _*)

// Required for assembly to use with spark-shell
libraryDependencies += "io.netty" % "netty-all" % "4.0.43.Final" force() excludeAll (excludedDeps: _*)

updateOptions := updateOptions.value.withLatestSnapshots(false)

// That may not work properly without project/PackagingTypePlugin.scala
// FIXME Remove it if not needed
lazy val mavenProps = settingKey[Unit]("workaround for Maven properties")
mavenProps := {
  sys.props("envClassifier") = envClassifier.value
  sys.props("hbase.version") = hbaseVersion.value
  sys.props("hadoop.version") = hadoopVersion.value
  ()
}

val scalatestVer = "3.0.8"
libraryDependencies += "org.scalactic" %% "scalactic" % scalatestVer
libraryDependencies += "org.scalatest" %% "scalatest" % scalatestVer % Test
parallelExecution in Test := false

def spliceDep(name: String, classfr: String): ModuleID = {
  ("com.splicemachine" % name % spliceVersion)
    .classifier(classfr)
    .withSources()
    .excludeAll(excludedDeps: _*)
}
