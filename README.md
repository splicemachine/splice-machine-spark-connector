# Splice Machine Connector for Apache Spark™

**Splice Machine Connector for Apache Spark™** is an [Apache Spark™](https://spark.apache.org/) connector to work with data in [Splice Machine](https://www.splicemachine.com/).

The connector supports batch and streaming queries using the following Data Source contracts in [Spark SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html) and [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html):

1. `spliceV1` format for Data Source V1 API

## Status

This connector is currently work-in-progress and is likely to get backwards-incompatible updates.

Consult [Issues](https://github.com/jaceklaskowski/splice-machine-spark-connector/issues) to know the missing features.

## Requirements

1. Install [sbt](https://www.scala-sbt.org/) build tool

1. Install [Apache Spark](https://spark.apache.org/)

1. Install [Splice Machine](https://www.splicemachine.com/product/)

1. [Java 8](https://openjdk.java.net/install/)

## Building

You have to build the data source yourself before first use using `sbt package` command.

```
$ sbt package
...
[info] Packaging .../target/scala-2.11/splice-machine-spark-connector_2.11-0.1.jar ...
[info] Done packaging.
```

Once done, the jar file (`target/scala-2.11/splice-machine-spark-connector_2.11-0.1.jar` above) is the connector.

Optionally, you could `sbt publishLocal` to publish the connector to the local repository, i.e. `~/.ivy2/local`.

**CAUTION**: You may need to run `sbt package` twice (`sbt update` actually) due to some dependencies not being downloaded properly. It is an issue with the build tool (sbt) and Maven properties to resolve proper dependencies.

## Running

There are a couple of ways to use the connector in your Spark application:

1. **(recommended)** Define it as a project dependency and `sbt assembly` to create an uber-jar with the classes of the project and the connector

1. Use `spark-submit --packages` command-line option (after `sbt publishLocal`)

1. Use `spark-submit --jars [connector-jar-file] [your-app]`

**CAUTION**: The versions of Scala that were used to build the connector and Apache Spark have to match (e.g. `2.11`, `2.12`).

## Testing

Make sure you are using Java 8 (or the environment is not going to boot up).

```
$ java -version
openjdk version "1.8.0_222"
OpenJDK Runtime Environment (AdoptOpenJDK)(build 1.8.0_222-b10)
OpenJDK 64-Bit Server VM (AdoptOpenJDK)(build 25.222-b10, mixed mode)
```

Start Splice Machine first, e.g. `./start-splice-cluster -p cdh5.14.0 -bl`.
Remove `-bl` options unless you are starting the Splice Machine instance for the very first time.

```
// In Splice's home directory
$ ./start-splice-cluster -p cdh5.14.0
Running Splice insecure,cdh5.14.0 master and 2 regionservers with CHAOS = false in:
   $SPLICE_HOME/platform_it
Starting ZooKeeper. Log file is $SPLICE_HOME/platform_it/zoo.log
Starting YARN. Log file is $SPLICE_HOME/platform_it/yarn.log
Starting Kafka. Log file is $SPLICE_HOME/platform_it/kafka.log
Starting Master and 1 Region Server. Log file is $SPLICE_HOME/platform_it/splice.log
  Waiting. . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . .
Starting Region Server $SPLICE_HOME/platform_it/spliceRegionSvr2.log
  Waiting. . . . . .
```

Execute the integration tests using `sbt test` (or `sbt testOnly`).

```
// In the connector's home directory
$ sbt test
...
[info] Run completed in 25 seconds, 749 milliseconds.
[info] Total number of tests run: 8
[info] Suites: completed 4, aborted 0
[info] Tests: succeeded 8, failed 0, canceled 0, ignored 0, pending 0
[info] All tests passed.
[success] Total time: 39 s, completed Aug 23, 2019 10:15:23 AM
```

**NOTE**: For some reasons testing in IntelliJ IDEA may not always work. Use `sbt test` for reliable reproducible tests.

Use `./sqlshell.sh` to execute queries and verify the test results.

```
// In Splice's home directory
$ ./sqlshell.sh
...
SPLICE* - 	jdbc:splice://localhost:1527/splicedb
* = current connection
...
splice> show tables in splice;
TABLE_SCHEM         |TABLE_NAME                                        |CONGLOM_ID|REMARKS
-------------------------------------------------------------------------------------------------------
SPLICE              |SPLICEDATASOURCEV1BATCHSPEC                       |1584      |
SPLICE              |SPLICEDATASOURCEV1STREAMINGSPEC                   |1600      |
SPLICE              |SPLICESPEC                                        |1664      |

3 rows selected

splice> select * from SPLICESPEC;
ID                  |TEST_NAME
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
0                   |SpliceSpec

1 row selected
```

After you're done with tests, you can stop Splice Machine using `./start-splice-cluster -k`.

## spark-shell

You should build the data source using `sbt assembly` command.

```
$ sbt assembly
...
[success] Total time: 27 s, completed Nov 13, 2019 10:00:09 AM
```

Once done, the assembly jar file (`target/scala-2.11/splice-machine-spark-connector-assembly-0.1.jar`) is the connector.

The below session uses `spark-shell` for demonstration purposes.

**NOTE**: Start Splice Machine, e.g. `./start-splice-cluster -p cdh5.14.0 -bl`.

```
// In Splice's home directory

// Create table first
$ ./sqlshell.sh

splice> create table t1 (id int, name varchar(50));
0 rows inserted/updated/deleted

splice> insert into t1 values (0, 'The connector works!');
1 row inserted/updated/deleted
```

**CAUTION**: `spark-shell` from Apache Spark 2.2 (with Hadoop 2.6 or compatible version), e.g. [Apache Spark 2.2.3](https://archive.apache.org/dist/spark/spark-2.2.3/), does not work.

```
$ spark-shell --version
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.4.4
      /_/

Using Scala version 2.11.12, OpenJDK 64-Bit Server VM, 1.8.0_222
Branch
Compiled by user  on 2019-08-27T21:21:38Z
Revision
Url
Type --help for more information.

$ spark-shell --driver-class-path target/scala-2.11/splice-machine-spark-connector-assembly-0.1.jar

assert(spark.version == "2.4.4", "The connector works just fine with Spark 2.4.4")

assert(spark.version != "2.2.3", "FIXME: The connector does not work with Spark 2.2.3") 

val user = "splice"
val password = "admin"
val url = s"jdbc:splice://localhost:1527/splicedb;user=$user;password=$password"
val table = "t1"
val t1 = spark.read.format("spliceV1").option("url", url).option("table", table).load

scala> t1.show
+---+--------------------+
| ID|                NAME|
+---+--------------------+
|  0|The connector works!|
+---+--------------------+
```
