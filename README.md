# Splice Machine Connector for Apache Spark™

**Splice Machine Connector for Apache Spark™** is a library to allow Apache Spark (namely Spark SQL and Spark Structured Streaming) to work with data in Splice Machine.

The connector supports the following Data Source contracts in Spark SQL:

1. `spliceV1` format for Data Source V1 API

## Status

This library is currently work-in-progress and is likely to get backwards-incompatible updates.

Consult [Issues](https://github.com/jaceklaskowski/splice-machine-spark-connector/issues) to know the missing features.

## Requirements

1. Install [sbt](https://www.scala-sbt.org/) build tool

1. Install [Apache Spark](https://spark.apache.org/)

## Building

You have to build the data source yourself before first use using `sbt package` command.

```
$ sbt package
...
[info] Packaging .../target/scala-2.11/splice-machine-spark-connector_2.11-0.1.jar ...
[info] Done packaging.
```

Once done, the jar file is the connector.

Optionally, you could `sbt publishLocal` to publish the connector to the local repository, i.e. `~/.ivy2/local`.

## Running

There are a couple of ways to use the connector:

1. Use `spark-submit --jars [connector-jar-file]` 

1. If you `sbt publishLocal` you could use `spark-submit --packages` command-line option

1. Define it as a project dependency and `sbt assemble` to create an uber-jar with the classes of the project and the connector

**CAUTION**: The versions of Scala that were used to build the connector and Apache Spark (e.g. `2.11`, `2.12`) have to match.

The below session uses `spark-shell` for demonstration purposes.

```
$ spark-shell --jars target/scala-2.11/splice-machine-spark-connector_2.11-0.1.jar
...
scala> spark.version
res0: String = 2.4.1

scala> spark.read.format("spliceV1").load.show
+---+--------------+                                                            
| id|          name|
+---+--------------+
|  0|splice machine|
+---+--------------+
```

## Testing

Use `sbt test` (or `sbt testOnly`) to execute the integration tests.

**NOTE**: Don't forget to start Splice Machine before the tests, e.g. `./start-splice-cluster -p cdh5.14.0 -bl`.

```
$ cd $SPLICE_HOME

$ ./start-splice-cluster
...
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
...
Running Splice insecure,cdh5.14.0 master and 2 regionservers with CHAOS = false in:
   $SPLICE_HOME/platform_it
Starting ZooKeeper. Log file is $SPLICE_HOME/platform_it/zoo.log
Starting YARN. Log file is $SPLICE_HOME/platform_it/yarn.log
Starting Kafka. Log file is $SPLICE_HOME/platform_it/kafka.log
Starting Master and 1 Region Server. Log file is $SPLICE_HOME/platform_it/splice.log
  Waiting. . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . .
Starting Region Server $SPLICE_HOME/platform_it/spliceRegionSvr2.log
  Waiting. . . . . .

$ sbt test
...
[info] Run completed in 4 seconds, 415 milliseconds.
[info] Total number of tests run: 3
[info] Suites: completed 4, aborted 0
[info] Tests: succeeded 3, failed 0, canceled 0, ignored 1, pending 0
[info] All tests passed.
```

The following command is what is currently under *heavy development*:

```
$ sbt clean 'testOnly *SpliceDataSourceV1BatchSpec'
...
INFO SpliceSpark: Splice Client in SpliceSpark true
...
[info] SpliceDataSourceV1BatchSpec:
[info] Splice Machine Connector (Data Source API V1 / Batch Mode)
[info] - should support batch reading with explicit schema
[info] - should throw an IllegalStateException when required options (e.g. url) are not defined
[info] Run completed in 6 seconds, 813 milliseconds.
[info] Total number of tests run: 2
[info] Suites: completed 1, aborted 0
[info] Tests: succeeded 2, failed 0, canceled 0, ignored 0, pending 0
[info] All tests passed.
```

**NOTE**: After you're done with tests, stop Splice Machine using `./start-splice-cluster -k`.

