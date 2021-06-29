name := "splice-machine-spark-connector"

val spliceVersion = "3.1.0.2016"

version := spliceVersion

scalaVersion := "2.12.10"
//scalaVersion := "2.11.6"

lazy val scalaMajorVersion = settingKey[String]("")
scalaMajorVersion := "2.12"

// https://github.com/sbt/sbt/issues/5046
ThisBuild / useCoursier := false

lazy val envClassifier = settingKey[String]("")
//envClassifier := "cdh6.3.0"
envClassifier := "dbaas3.0"

lazy val distro = settingKey[String]("")
distro := "cdh6.3.2"

lazy val hbaseVersion = settingKey[String]("")
//hbaseVersion := s"1.2.0-${envClassifier.value}"
hbaseVersion := s"2.1.0-${distro.value}"

lazy val hadoopVersion = settingKey[String]("")
//hadoopVersion := s"2.6.0-${envClassifier.value}"
hadoopVersion := s"3.0.0-${distro.value}"

lazy val kafkaVersion = settingKey[String]("")
kafkaVersion := s"2.2.1-${distro.value}"

// FIXME hbase_sql should actually be dependency of splice_spark
//  ClassNotFoundException: com.splicemachine.derby.impl.SpliceSpark

// FIXME hbase_storage should actually be dependency of splice_spark
// java.lang.NoClassDefFoundError: com/splicemachine/access/HConfiguration
// https://stackoverflow.com/a/46763742/1305344

val excludedDepsNonSpark = Seq(
  ExclusionRule(organization = "org.xerial.snappy", name = "snappy-java"),
  ExclusionRule(organization = "tomcat", name = "jasper-compiler"),
  // Added later separately
  ExclusionRule(organization = "com.splicemachine", name = "scala_util"),
  ExclusionRule(organization = "javax.ws.rs", name = "javax.ws.rs-api"),
  ExclusionRule(organization = "org.apache.kafka", name = "kafka_2.11"),
  ExclusionRule(organization = "org.scala-lang.modules", name = "scala-parser-combinators_2.11")
)

val excludedDeps = excludedDepsNonSpark ++ Seq(
  // FIXME Somehow 2.2.0 is pulled down
  ExclusionRule(organization = "org.apache.spark"),
)

libraryDependencies ++= Seq(
  "splice_spark2",
  "hbase_sql",
  "hbase_storage",
  "hbase_pipeline",
  "spark_sql"
).map(spliceDep(_, envClassifier.value))

libraryDependencies ++= Seq(
  "db-engine"
).map(spliceDep(_, ""))

lazy val printLibDep = taskKey[Unit]("")
printLibDep := libraryDependencies.value.sortBy(_.toString).foreach(println)

lazy val sparkVersion = settingKey[String]("")
//sparkVersion := s"2.4.0-${envClassifier.value}"
sparkVersion := s"3.0.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion.value % Provided

// Somehow it is necessary to add the dependency implicitly
// After it was excluded explicitly from all dependencies
lazy val scalaUtilClassifier = Def.setting {
  s"${envClassifier.value}-${sparkVersion.value}_${scalaBinaryVersion.value}"
}
libraryDependencies += ("com.splicemachine" % "scala_util" % spliceVersion)
  .classifier(scalaUtilClassifier.value)
  .withSources()

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion.value excludeAll(excludedDeps: _*)

// Required to ensure proper dependency
// (otherwise cdh5.12.0 version was resolved and used)
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion.value excludeAll (excludedDeps: _*)
libraryDependencies += "org.apache.hbase" % "hbase-server" % hbaseVersion.value excludeAll (excludedDeps: _*)
libraryDependencies += "org.apache.hbase" % "hbase-common" % hbaseVersion.value excludeAll (excludedDeps: _*)
libraryDependencies += "org.apache.hbase" % "hbase-hadoop-compat" % hbaseVersion.value excludeAll (excludedDeps: _*)
libraryDependencies += "org.apache.hbase" % "hbase-zookeeper" % hbaseVersion.value excludeAll (excludedDeps: _*)
libraryDependencies += "org.apache.hbase" % "hbase-mapreduce" % hbaseVersion.value excludeAll (excludedDeps: _*)
libraryDependencies += "org.apache.kafka" % "kafka_2.12" % kafkaVersion.value excludeAll (excludedDeps: _*)
libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % sparkVersion.value excludeAll (excludedDepsNonSpark: _*)
libraryDependencies += "org.scala-lang.modules" % "scala-parser-combinators_2.12" % "1.1.2" excludeAll (excludedDeps: _*)
libraryDependencies += "org.apache.activemq" % "activemq-all" % "5.12.0" excludeAll (excludedDeps: _*)
libraryDependencies += "com.rabbitmq.jms" % "rabbitmq-jms" % "1.11.0" excludeAll (excludedDeps: _*)
libraryDependencies += "io.confluent" % "kafka-jms-client" % "5.1.0" excludeAll (excludedDeps: _*)
libraryDependencies += "org.apache.geronimo.specs" % "geronimo-jms_1.1_spec" % "1.1" excludeAll (excludedDeps: _*)
libraryDependencies += "com.ibm.mq" % "com.ibm.mq.allclient" % "9.2.2.0" excludeAll (excludedDeps: _*)
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.1" excludeAll (excludedDeps: _*)

// For development only / local Splice SNAPSHOTs
resolvers += Resolver.mavenLocal
resolvers +=
  ("splicemachine-public" at "http://repository.splicemachine.com/nexus/content/groups/public")
    .withAllowInsecureProtocol(true)
resolvers +=
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
resolvers +=
  "Confluent Repository" at "https://packages.confluent.io/maven/"

// com.fasterxml.jackson.databind.JsonMappingException: Incompatible Jackson version: 2.9.2
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.10.0" force() excludeAll (excludedDeps: _*)

// Required for assembly to use with spark-shell
libraryDependencies += "io.netty" % "netty-all" % "4.1.17.Final" force() excludeAll (excludedDeps: _*)

//libraryDependencies += "javax.ws.rs" % "javax.ws.rs-api" % "2.1" excludeAll (excludedDeps: _*)

updateOptions := updateOptions.value.withLatestSnapshots(false)

// That may not work properly without project/PackagingTypePlugin.scala
// FIXME Remove it if not needed
lazy val mavenProps = settingKey[Unit]("workaround for Maven properties")
mavenProps := {
  sys.props("envClassifier") = envClassifier.value
  sys.props("distro") = distro.value
  sys.props("hbase.version") = hbaseVersion.value
  sys.props("hadoop.version") = hadoopVersion.value
  sys.props("kafka.version") = kafkaVersion.value
  sys.props("scala.binary.version") = scalaMajorVersion.value
  ()
}

val scalatestVer = "3.1.0"
libraryDependencies += "org.scalactic" %% "scalactic" % scalatestVer
libraryDependencies += "org.scalatest" %% "scalatest" % scalatestVer % Test
parallelExecution in Test := false

def spliceDep(name: String, classfr: String): ModuleID = {
  ("com.splicemachine" % name % spliceVersion)
    .classifier(classfr)
    .withSources()
    .excludeAll(excludedDeps: _*)
}

Test / fork := true
