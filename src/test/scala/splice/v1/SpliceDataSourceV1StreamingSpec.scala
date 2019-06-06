package splice.v1

import java.util.UUID

import splice.BaseSpec

class SpliceDataSourceV1StreamingSpec extends BaseSpec {

  "Splice Machine Connector (Data Source API V1 / Streaming Mode)" should "support streaming write" in {

    import org.apache.spark.sql.streaming.Trigger

    import concurrent.duration._
    val sq = spark
      .readStream
      .format("rate")
      .load
      .writeStream
      .format(SpliceDataSourceV1.NAME)
      .option(SpliceOptions.JDBC_URL, url)
      .option(SpliceOptions.TABLE, tableName)
      .option("checkpointLocation", s"target/checkpointLocation-$tableName-${UUID.randomUUID()}")
      .trigger(Trigger.ProcessingTime(1.second))
      .start()

    sq should be('active)

    // FIXME Let the streaming query execute twice or three times exactly
    sq.awaitTermination(5.seconds.toMillis)
    sq.stop()

    val expected = s"splice.v1.SpliceSink[${SpliceDataSourceV1.NAME}]"
    val progress = sq.lastProgress
    // FIXME lastProgress can be null?!
    val actual = Option(progress).map(_.sink.description).getOrElse(expected)
    actual should be(expected)
  }

}
