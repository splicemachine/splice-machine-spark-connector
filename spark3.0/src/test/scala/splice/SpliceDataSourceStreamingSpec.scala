package splice

import java.util.UUID

import _root_.com.splicemachine.spark2.splicemachine.SplicemachineContext
import org.apache.spark.SparkContext

import concurrent.duration._
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row}
import org.apache.spark.sql.streaming.StreamingQuery

class SpliceDataSourceStreamingSpec extends BaseSpec {
// TODO reconcile with hdp3.1.0 version
  def sparkMajorVer: Float = {  // convert like "2.4.0" string to 2.4 float
    val ver = spark.version.split("[.]")
    (ver(0)+"."+ver(1)).toFloat
  }
  
  def unsupported = println( s" *** WARN This functionality not supported for Spark ${spark.version} ***" )

  "Splice Machine Connector (Streaming Mode)" should "support streaming write with foreachBatch" in {
    if( sparkMajorVer < 2.4 ) { unsupported }
    else {
      val sq = spark
        .readStream
        .format("rate")
        .load
        .writeStream
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          println(s">>> batchDF=${batchDF.getClass.getName}")
          batchDF.write
            .format(SpliceDataSource.NAME)
            .option(SpliceOptions.JDBC_URL, url)
            .option(SpliceOptions.TABLE, tableName)
            .save
        }.start()

      handleSQ(sq, "ForeachBatch")
    }
  }
  
  "Splice Machine Connector (Streaming Mode)" should "support streaming write with ForeachWriter" in {
    if (sparkMajorVer < 2.0) { unsupported }
    else {
      import org.apache.spark.sql.streaming.Trigger

      // Copy base class vals to local ones to avoid FlatSpec serialization issue in spark
      val jdbcUrl = url
      val table = tableName

      val sq = spark
        .readStream
        .format("rate")
        .load
        .writeStream
        .option("checkpointLocation", s"target/checkpointLocation-$tableName-${UUID.randomUUID()}")
        .trigger(Trigger.ProcessingTime(1.second))
        .foreach(
          new ForeachWriter[Row] {
            var spliceCtx: SplicemachineContext = _
            var sparkContext: SparkContext = _
  
            def open(partitionId: Long, version: Long): Boolean = {
              spliceCtx = new SplicemachineContext(jdbcUrl)
              sparkContext = SparkContext.getOrCreate
              true
            }
  
            def process(record: Row): Unit =
              spliceCtx.insert(
                sparkContext.parallelize(Seq(record)),
                record.schema,
                table
              )
  
            def close(errorOrNull: Throwable): Unit = {}
          }
        )
        .start()

      handleSQ(sq, "ForeachWriter")
    }
  }

  "Splice Machine Connector (Streaming Mode)" should "support streaming write with SpliceSink" in {
    if (sparkMajorVer > 2.2) { unsupported }
    else {
      import org.apache.spark.sql.streaming.Trigger

      val sq = spark
        .readStream
        .format("rate")
        .load
        .writeStream
        .format(SpliceDataSource.NAME)
        .option(SpliceOptions.JDBC_URL, url)
        .option(SpliceOptions.TABLE, tableName)
        .option("checkpointLocation", s"target/checkpointLocation-$tableName-${UUID.randomUUID()}")
        .trigger(Trigger.ProcessingTime(1.second))
        .start()

      handleSQ(sq, s"splice.SpliceSink[${SpliceDataSource.NAME}]")
    }
  }

  def handleSQ(sq: StreamingQuery, expected: String) = {
    sq should be('active)

  // FIXME Let the streaming query execute twice or three times exactly
    sq.awaitTermination(10.seconds.toMillis)
    sq.stop()

    val progress = sq.lastProgress
    // FIXME lastProgress can be null?!
    val actual = Option(progress).map(_.sink.description).getOrElse(expected)
    actual.substring(0, expected.length) should be(expected)
  }

}
