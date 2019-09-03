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

assemblyExcludedJars in assembly := {
  val excludedDeps = Set(
    // Conflicts with splice_aws-2.8.0.1926-shade.jar
    "aws-java-sdk-bundle-1.11.134.jar",
    "aws-java-sdk-kms-1.11.82.jar",
    "ion-java-1.0.1.jar",
    // Conflicts with hadoop-core-2.6.0-mr1-cdh5.14.0.jar
    "hadoop-mapreduce-client-core-2.6.0-cdh5.14.0.jar",
    "hadoop-mapreduce-client-common-2.6.0-cdh5.12.0.jar",
    // Conflicts with Spark 2.2.0.cloudera2
    // FIXME What dep brings Spark 2.1.0?
    "spark-yarn_2.11-2.1.0.jar",
    // Conflicts with commons-beanutils-1.7.0.jar
    // http://commons.apache.org/proper/commons-beanutils/
    // Bean Collections has an additional dependency on Commons Collections.
    "commons-collections-3.2.2.jar",
    // Conflicts with commons-beanutils-core-1.8.0.jar
    // NOTE a dependency version mismatch
    "commons-beanutils-1.7.0.jar",
    // Included in / Conflicts with
    // jython-standalone-2.5.3.jar
    "jline-0.9.94.jar",
    "xercesImpl-2.9.1.jar",
    // Conflicts with jersey-core-1.9.jar
    "javax.ws.rs-api-2.0.1.jar",
    // Included in / Conflicts with
    // jcl-over-slf4j-1.7.5.jar
    "commons-logging-1.2.jar",
    // Included in / Conflicts with
    // aopalliance-repackaged-2.4.0-b34.jar
    "aopalliance-1.0.jar",
    // Included in / Conflicts with
    // servlet-api-2.5-6.1.14.jar
    "servlet-api-2.5-20081211.jar",
    // Included in / Conflicts with
    // servlet-api-2.5-6.1.14.jar from Jetty
    "servlet-api-2.5.jar",
    "jsp-api-2.1-6.1.14.jar",
    "jsp-api-2.1.jar",
    // Included in / Conflicts with
    // hbase_sql-2.8.0.1926-cdh5.14.0.jar
    // jython-standalone-2.5.3.jar
    "guava-20.0.jar",
    // Included in / Conflicts with
    // jersey-server-2.22.2.jar
    // NOTE a dependency version mismatch
    "jersey-server-1.9.jar",
    // Included in / Conflicts with
    // servlet-api-2.5-6.1.14.jar from Jetty
    // NOTE a dependency version mismatch
    "javax.servlet-api-3.1.0.jar",
    // Included in / Conflicts with
    // javax.inject-2.4.0-b34.jar from Glassfish
    "javax.inject-1.jar",
    // Others
    "jasper-runtime-5.5.23.jar",
    "findbugs-annotations-1.3.9-1.jar"
  )
  val cp = (fullClasspath in assembly).value
  cp filter { f => excludedDeps.contains(f.data.getName) }
}