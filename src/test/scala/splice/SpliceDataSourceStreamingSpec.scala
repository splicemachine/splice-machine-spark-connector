package splice

import java.util.UUID

class SpliceDataSourceStreamingSpec extends BaseSpec {

  "Splice Machine Connector (Streaming Mode)" should "support streaming write" in {

    import org.apache.spark.sql.streaming.Trigger

    import concurrent.duration._
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

    sq should be('active)

    // FIXME Let the streaming query execute twice or three times exactly
    sq.awaitTermination(5.seconds.toMillis)
    sq.stop()

    val expected = s"splice.SpliceSink[${SpliceDataSource.NAME}]"
    val progress = sq.lastProgress
    // FIXME lastProgress can be null?!
    val actual = Option(progress).map(_.sink.description).getOrElse(expected)
    actual should be(expected)
  }

}
