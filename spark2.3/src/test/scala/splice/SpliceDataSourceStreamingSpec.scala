package splice

import java.util.UUID

import com.splicemachine.spark2.splicemachine.SplicemachineContext
import org.apache.spark.SparkContext

import concurrent.duration._
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{StructField, StructType}

class SpliceDataSourceStreamingSpec extends BaseSpec {
// TODO reconcile with hdp3.1.0 version
  def sparkMajorVer: Float = {  // convert like "2.4.0" string to 2.4 float
    val ver = spark.version.split("[.]")
    (ver(0)+"."+ver(1)).toFloat
  }
  
  def unsupported = println( s" *** WARN This functionality not supported for Spark ${spark.version} ***" )

  "Splice Machine Connector (Streaming Mode)" should "support streaming write with foreachBatch" in {
    if( sparkMajorVer < 2.4 ) { unsupported }
//    else {
//      val sq = spark
//        .readStream
//        .format("rate")
//        .load
//        .writeStream
//        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
//          println(s">>> batchDF=${batchDF.getClass.getName}")
//          batchDF.write
//            .format(SpliceDataSource.NAME)
//            .option(SpliceOptions.JDBC_URL, url)
//            .option(SpliceOptions.TABLE, tableName)
//            .save
//        }.start()
//
//      handleSQ(sq, "ForeachBatch")
//    }
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
            var tableExists: Boolean = _
//            var spliceRelation: SpliceRelation = _
  
            def open(partitionId: Long, version: Long): Boolean = {
              spliceCtx = new SplicemachineContext(jdbcUrl)
              sparkContext = SparkContext.getOrCreate
              tableExists = spliceCtx.tableExists(table)
//              spliceRelation = new SpliceRelation(
//                new StructType(),
//                new SpliceOptions(Map(
//                  SpliceOptions.JDBC_URL -> jdbcUrl, 
//                  SpliceOptions.TABLE -> table
//                ))
//              )(SparkSession.builder.getOrCreate)
              true
            }
            
            def process(record: Row): Unit = {
              if( ! tableExists ) {
                spliceCtx.createTable(
                  table,
                  StructType( record.schema.map(f => StructField(f.name.toUpperCase,f.dataType,f.nullable,f.metadata)) )
                )
                tableExists = true
              }
              spliceCtx.insert(
                sparkContext.parallelize(Seq(record)),
                record.schema,
                table
              )
            }
      //              spliceRelation.insert(
            //                sparkContext.parallelize(Seq(record)),
            //                false
            //              )
  
            def close(errorOrNull: Throwable): Unit = {}
          }
        )
        .start()

      handleSQ( sq,
        if(sparkMajorVer < 2.4) "ForeachSink" else "ForeachWriterProvider"
      )
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
    val actual = Option(progress)
      .map(_.sink.description)
      .map(desc => if(desc.length > expected.length) {desc.substring(0, expected.length)} else desc)
      .getOrElse(expected)

    actual should be(expected)
  }

}
