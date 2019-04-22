package splice.v1

import splice.BaseSpec
import org.scalatest.Ignore

@Ignore
class SpliceDataSourceV1StreamingSpec extends BaseSpec {

  "Splice Machine Connector (Data Source API V1 / Streaming Mode)" should "support streaming write (with extra session-scoped options)" in
    withSparkSession { spark =>

      // FIXME Make sure that the options are passed on
      spark.conf.set("spark.datasource.splice.session.option", "session-value")

      import java.util.UUID

      import org.apache.spark.sql.streaming.Trigger

      import concurrent.duration._
      val sq = spark
        .readStream
        .format("rate")
        .load
        .writeStream
        .format(SpliceDataSourceV1.NAME)
        .option("splice.option", "option-value")
        .option("checkpointLocation", s"target/checkpointLocation-${UUID.randomUUID()}")
        .trigger(Trigger.ProcessingTime(1.second))
        .start()

      sq should be('active)

      // FIXME Let the streaming query execute twice or three times exactly
      import concurrent.duration._
      sq.awaitTermination(2.seconds.toMillis)
      sq.stop()

      sq should not be 'active

      val progress = sq.lastProgress
      val actual = progress.sink.description
      val expected = s"splice.v1.SpliceSink[${SpliceDataSourceV1.NAME}]"
      actual should be(expected)
    }

}
