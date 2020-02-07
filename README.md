# Splice Machine Connector for Apache Spark™

**Splice Machine Connector for Apache Spark™** is an [Apache Spark™](https://spark.apache.org/) connector to process data in tables in [Splice Machine](https://www.splicemachine.com/).

The connector supports loading and saving datasets in batch ([Spark SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html)) and streaming ([Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)) queries.
 
In order to use the connector simply reference `splice` as the format while loading or saving datasets.

## Status

This connector is currently work-in-progress and is likely to get backwards-incompatible updates.

Consult [Issues](https://github.com/jaceklaskowski/splice-machine-spark-connector/issues) to know the missing features.

## Requirements

1. [Java 8](https://openjdk.java.net/install/)

1. [sbt](https://www.scala-sbt.org/)

1. [Apache Spark](https://spark.apache.org/)

1. [Splice Machine](https://www.splicemachine.com/product/)

## Building

You have to build the data source yourself before first use using `sbt package` command.

```
$ sbt package
...
[success] Total time: 9 s, completed Feb 5, 2020 6:56:19 PM
```

You should have the connector available as `target/scala-2.11/splice-machine-spark-connector_2.11-0.1.jar`.

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
$ ./start-splice-cluster -p cdh5.14.0 -bl
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

**TIP**: Monitor `$SPLICE_HOME/platform_it/splice.log` log file.

Execute the integration tests using `sbt test` (or `sbt testOnly`).

```
// In the connector's home directory
$ sbt test
...
[info] Run completed in 19 seconds, 469 milliseconds.
[info] Total number of tests run: 8
[info] Suites: completed 4, aborted 0
[info] Tests: succeeded 8, failed 0, canceled 0, ignored 0, pending 0
[info] All tests passed.
[success] Total time: 25 s, completed Feb 5, 2020 7:01:09 PM
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
SPLICE              |SPLICEDATASOURCEBATCHSPEC                         |1808      |
SPLICE              |SPLICEDATASOURCESTREAMINGSPEC                     |1856      |
SPLICE              |SPLICESPEC                                        |1840      |

3 rows selected

splice> select * from SPLICESPEC;
ID                  |TEST_NAME
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
0                   |SpliceSpec

1 row selected
```

After you're done with tests, you can stop Splice Machine using `./start-splice-cluster -k`.

## spark-shell (Spark 2.2.0 w/ Hadoop 2.6)

**NOTE**: [spark-2.2.0-bin-hadoop2.6.tgz](https://archive.apache.org/dist/spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.6.tgz) was tested to work fine (see [Allow spark-shell to be used to save batch dataframe to splice table](https://github.com/jaceklaskowski/splice-machine-spark-connector/issues/14)).

```
$ spark-shell --version
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.2.0
      /_/

Using Scala version 2.11.8, OpenJDK 64-Bit Server VM, 1.8.0_222
Branch
Compiled by user jenkins on 2017-06-30T22:58:04Z
Revision
Url
Type --help for more information.
```

**TIP**: Make sure to use the proper versions of Apache Spark 2.2.0, Scala 2.11 and Java 1.8.0.

You should build the data source using `sbt assembly` command.

```
$ sbt assembly
...
[success] Total time: 28 s, completed Feb 5, 2020 7:03:12 PM
```

You should have the connector assembled as `target/scala-2.11/splice-machine-spark-connector-assembly-0.1.jar`.

**NOTE**: Start Splice Machine, e.g. `./start-splice-cluster -p cdh5.14.0 -bl`.

**NOTE**: The following `CREATE TABLE` and `INSERT` SQL statements are optional since the connector could be used to create a splice table and save (_insert_) rows instead. 

```
// In Splice's home directory

// Create table first
$ ./sqlshell.sh

splice> create table t1 (id int, name varchar(50));
0 rows inserted/updated/deleted

splice> insert into t1 values (0, 'The connector works!');
1 row inserted/updated/deleted
```

**NOTE** Make sure you use `spark-2.2.0-bin-hadoop2.6` or compatible.

```
// You should be using ASSEMBLY jar
$ spark-shell \
    --jars target/scala-2.11/splice-machine-spark-connector-assembly-0.1.jar \
    --driver-class-path target/scala-2.11/splice-machine-spark-connector-assembly-0.1.jar

val compatibleSparkVersion = "2.2.0"
assert(
    spark.version == compatibleSparkVersion,
    s"The connector works just fine with Spark $compatibleSparkVersion")

val user = "splice"
val password = "admin"
val url = s"jdbc:splice://localhost:1527/splicedb;user=$user;password=$password"
val table = "t1"
val t1 = spark.read.format("splice").option("url", url).option("table", table).load

// You may see some WARNs, please disregard them
// In the end, spark-shell should give you t1 dataframe
// t1: org.apache.spark.sql.DataFrame = [ID: int, NAME: string]

// The following should display the splice table
t1.show

//
// Saving batch dataset to splice table
//

val ds = Seq((1, "Insert from spark-shell")).toDF("id", "name")
ds.write.format("splice").option("url", url).option("table", table).save

// The new rows inserted should be part of the output
t1.show
```

The above may trigger some WARN messages that you should simply disregard.

```
20/02/05 18:38:33 WARN ClientCnxn: Session 0x0 for server null, unexpected error, closing socket connection and attempting reconnect
java.net.ConnectException: Connection refused
	at sun.nio.ch.SocketChannelImpl.checkConnect(Native Method)
	at sun.nio.ch.SocketChannelImpl.finishConnect(SocketChannelImpl.java:717)
	at org.apache.zookeeper.ClientCnxnSocketNIO.doTransport(ClientCnxnSocketNIO.java:361)
	at org.apache.zookeeper.ClientCnxn$SendThread.run(ClientCnxn.java:1081)
```

You could also insert new records to the `t1` table, and `t1.show` should include them in the output next time you execute it.

```
splice> insert into t1 values (2, 'Insert from sqlshell');
1 row inserted/updated/deleted
```

```
scala> t1.show(truncate = false)
>>> [SpliceRelation.buildScan] Registering the splice JDBC driver
+---+-----------------------+
|ID |NAME                   |
+---+-----------------------+
|1  |Insert from spark-shell|
|0  |The connector works!   |
|2  |Insert from sqlshell   |
+---+-----------------------+
```

## Demo: Structured Queries (Spark SQL) with Kafka Data Source

The following demo shows how to use `spark-shell` to execute a structured query over a dataset from Apache Kafka (via [kafka data source](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)).

```
spark-shell \
  --jars target/scala-2.11/splice-machine-spark-connector-assembly-0.1.jar \
  --driver-class-path target/scala-2.11/splice-machine-spark-connector-assembly-0.1.jar \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0
```

The demo uses `t1` topic with a Kafka broker listening to `9092` port. The name of the splice table is `kafka`.

```
// Load a dataset from Kafka
val values = spark
  .read
  .format("kafka")
  .option("subscribe", "t1")
  .option("kafka.bootstrap.servers", ":9092")
  .load
  .select($"value" cast "string")

val user = "splice"
val password = "admin"
val url = s"jdbc:splice://localhost:1527/splicedb;user=$user;password=$password"

// Save the dataset to a splice table
values
  .write
  .format("splice")
  .option("url", url)
  .option("table", "kafka")
  .save()

// Check out the splice table

// You can use `sqlshell` of Splice Machine
// Or better query the table using Spark SQL
spark
  .read
  .format("splice")
  .option("url", url)
  .option("table", "kafka")
  .load
  .show
```

## Demo: Streaming Queries (Spark Structured Streaming) with Kafka Data Source

The following demo shows how to use `spark-shell` to execute a streaming query over datasets from Apache Kafka (via [kafka data source](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)).

```
spark-shell \
  --jars target/scala-2.11/splice-machine-spark-connector-assembly-0.1.jar \
  --driver-class-path target/scala-2.11/splice-machine-spark-connector-assembly-0.1.jar \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0
```

The demo uses `t1` topic with a Kafka broker listening to `9092` port. The name of the splice table is `kafka`.

```
val values = spark
  .readStream
  .format("kafka")
  .option("subscribe", "t1")
  .option("kafka.bootstrap.servers", ":9092")
  .load
  .select($"value" cast "string")

assert(values.isStreaming)

val user = "splice"
val password = "admin"
val url = s"jdbc:splice://localhost:1527/splicedb;user=$user;password=$password"

values
  .writeStream
  .format("splice")
  .option("url", url)
  .option("table", "kafka")
  .option("checkpointLocation", "/tmp/splice-checkpointLocation")
  .start

// After you started the streaming query
// The splice table is constantly updated with new records from Kafka
// Use kafka-console-producer.sh --broker-list :9092 --topic t1 to send records to Kafka

// You can use `sqlshell` of Splice Machine
// Or better query the table using Spark SQL
spark
  .read
  .format("splice")
  .option("url", url)
  .option("table", "kafka")
  .load
  .show
```