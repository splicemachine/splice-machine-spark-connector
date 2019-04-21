# Splice Machine Connector for Apache Spark™

**Splice Machine Connector for Apache Spark™** is a library to allow Apache Spark (namely Spark SQL and Spark Structured Streaming) to work with data in Splice Machine.

The connector *is aimed to* support two different Data Source contracts in Apache Spark:

1. `spliceV1` format for Data Source V1 API 

1. `spliceV2` format for Data Source V2 API

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