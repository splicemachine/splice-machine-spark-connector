// Don't execute tests
test in assembly := {}

// Exclude duplicates

// NOTE excludeAll in build.sbt won't work
// Use assemblyMergeStrategy and assemblyExcludedJars
// * https://github.com/sbt/sbt-assembly/issues/35
// * https://stackoverflow.com/q/41894055/1305344

assemblyMergeStrategy in assembly := {
  // FIXME How do others deal with UnusedStubClass.class in so many Spark deps?
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.first
  // FIXME Why do the two come with the same package-info.class?
  // hadoop-yarn-api-2.6.0-cdh5.14.0.jar
  // hadoop-yarn-common-2.6.0-cdh5.14.0.jar
  case PathList("org", "apache", "hadoop", "yarn", ps @ _*) if ps.lastOption.exists(_ endsWith "package-info.class") => MergeStrategy.first
  // netty-all-4.0.43.Final.jar VS hadoop-hdfs-2.6.0-cdh5.14.0.jar
  case PathList("META-INF", "io.netty.versions.properties")  => MergeStrategy.first
  // db-engine-2.8.0.1926.jar VS splice_encoding-2.8.0.1926.jar
  case PathList("com", "splicemachine", "utils", _*)  => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".css"  => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

// FIXME There has to be a better way to exclude all the jars (!)
assemblyExcludedJars in assembly := {
  val excludedDeps = Set(
    // Conflicts with splice_aws-2.8.0.1926-shade.jar
    "aws-java-sdk-bundle-1.11.134.jar",
    "aws-java-sdk-kms-1.11.82.jar",
    "ion-java-1.0.1.jar",
    "httpclient-4.5.3.jar",
    // Conflicts with hadoop-core-2.6.0-mr1-cdh5.14.0.jar
    "hadoop-mapreduce-client-core-3.0.0-cdh6.3.0.jar",
    "hadoop-mapreduce-client-common-3.0.0-cdh6.3.0.jar",
    // Conflicts with Spark 2.2.0.cloudera2
    // FIXME What dep brings Spark 2.1.0?
    "spark-yarn_2.11-2.4.0-cdh6.3.0.jar",
    // Conflicts with commons-beanutils-1.7.0.jar
    // http://commons.apache.org/proper/commons-beanutils/
    // Bean Collections has an additional dependency on Commons Collections.
    "commons-collections-3.2.2.jar",
    // Conflicts with commons-beanutils-core-1.8.0.jar
    // NOTE a dependency version mismatch
    "commons-beanutils-1.9.3.jar",
    // Included in / Conflicts with
    // jython-standalone-2.5.3.jar
    "jline-2.12.jar",
    "xercesImpl-2.9.1.jar",
    // Conflicts with jersey-core-1.9.jar
    "javax.ws.rs-api-2.0.1.jar",
    // Included in / Conflicts with
    // jcl-over-slf4j-1.7.5.jar
    "commons-logging-1.1.1.jar",
    // Included in / Conflicts with
    // aopalliance-repackaged-2.4.0-b34.jar
    "aopalliance-1.0.jar",
    // Included in / Conflicts with
    // servlet-api-2.5-6.1.14.jar
    "servlet-api-2.5-20081211.jar",
    // Included in / Conflicts with
    // servlet-api-2.5-6.1.14.jar from Jetty
    "servlet-api-2.5.jar",
    "jsp-api-2.0.jar",
    "jsp-api-2.1.jar",
    // Included in / Conflicts with
    // hbase_sql-2.8.0.1926-cdh5.14.0.jar
    // jython-standalone-2.5.3.jar
    "guava-12.0.1.jar",
    // Included in / Conflicts with
    // jersey-server-2.22.2.jar
    // NOTE a dependency version mismatch
    "jersey-server-1.19.jar",
    // Included in / Conflicts with
    // servlet-api-2.5-6.1.14.jar from Jetty
    // NOTE a dependency version mismatch
    "javax.servlet-api-3.1.0.jar",
    // Included in / Conflicts with
    // javax.inject-2.4.0-b34.jar from Glassfish
    "javax.inject-1.jar",
    // FIXME It is assumed to exist earlier. Make sure nothing got broken
    // Have to be excluded to let shading in sbt-assembly work
    "jython-standalone-2.5.3.jar",
    // Exclude Spark jars
    "spark-launcher_2.11-2.4.0-cdh6.3.0.jar",
    "spark-yarn_2.11-2.4.0-cdh6.3.0.jar",
    "spark-network-common_2.11-2.4.0-cdh6.3.0.jar",
    "spark-network-shuffle_2.11-2.4.0-cdh6.3.0.jar",
    "spark-unsafe_2.11-2.4.0-cdh6.3.0.jar",
    "spark-tags_2.11-2.4.0-cdh6.3.0.jar",
    "spark-core_2.11-2.4.0-cdh6.3.0.jar",
    // FIXME Should spark-avro be excluded as well?
    "spark-avro_2.11-2.4.3.jar",
    // Hadoop
    "hadoop-mapreduce-client-jobclient-3.0.0-cdh6.3.0.jar",
    "hadoop-aws-2.6.0-cdh5.12.0.jar",
    "hadoop-yarn-server-common-3.0.0-cdh6.3.0.jar",
    "hadoop-hdfs-3.0.0-cdh6.3.0.jar",
    "hadoop-hdfs-3.0.0-cdh6.3.0-tests.jar",
    "hadoop-core-2.6.0-mr1-cdh5.14.0.jar",
    "hadoop-client-3.0.0-cdh6.3.0.jar",
    "hadoop-mapreduce-client-app-3.0.0-cdh6.3.0.jar",
    "hadoop-mapreduce-client-common-3.0.0-cdh6.3.0.jar",
    "hadoop-auth-3.0.0-cdh6.3.0.jar",
    "hadoop-mapreduce-client-shuffle-3.0.0-cdh6.3.0.jar",
    "hadoop-yarn-common-3.0.0-cdh6.3.0.jar",
    "hadoop-yarn-server-nodemanager-3.0.0-cdh6.3.0.jar",
    "hadoop-mapreduce-client-core-3.0.0-cdh6.3.0.jar",
    "hadoop-yarn-server-web-proxy-3.0.0-cdh6.3.0.jar",
    "hbase-hadoop2-compat-2.1.0-cdh6.3.0.jar",
    "hadoop-yarn-client-3.0.0-cdh6.3.0.jar",
    "hadoop-annotations-3.0.0-cdh6.3.0.jar",
    "hadoop-yarn-api-3.0.0-cdh6.3.0.jar",
//    "hadoop-common-3.0.0-cdh6.3.0.jar",
    // Others
    "jasper-runtime-5.5.23.jar",
    "findbugs-annotations-1.3.9-1.jar",
    "scalactic_2.11-3.0.8.jar",
    "scala-reflect-2.11.12.jar",
    "scala-xml_2.11-1.0.2.jar",
    "scala-parser-combinators_2.11-1.1.0.jar",
    "scala-compiler-2.11.12.jar",
    "hbase-shaded-netty-2.2.1.jar",
    "kryo-shaded-3.0.3.jar",
    "guava-20.0.jar",
    "annotations-2.0.3.jar",
    // Already in Spark
    "lz4-java-1.5.0.jar"
  )
  val cp = (fullClasspath in assembly).value
  cp filter { f =>
    val r = excludedDeps.contains(f.data.getName)
    val name = f.data.getName
    if (name.startsWith("protobuf-java") ||
//        name.startsWith("jython-standalone") ||
        name.startsWith("netty") ||
        name.startsWith("spark")) {
      println(s">>> Found: $name")
    }
    r
  }
}

// Shading protobuf-java-2.5.0.jar as it is part of Spark (and spark-shell)
// $SPARK_HOME/jars/protobuf-java-2.5.0.jar
// TIP Use getClass().getProtectionDomain().getCodeSource() to know the location of a class
assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.protobuf.**" -> "shaded.protobuf.@1").inAll,
  ShadeRule.rename("io.netty.**" -> "shaded.netty.@1").inAll,
  ShadeRule.rename("org.apache.hbase.thirdparty.io.netty.**" -> "shaded.netty.@1").inAll
)
//assemblyShadeRules in assembly := Seq(
//  ShadeRule.rename("com.google.protobuf.**" -> "shaded.protobuf.@1").inAll
//)
