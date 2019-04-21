package splice.v2

import splice.BaseSpec

class SpliceDataSourceV2Spec extends BaseSpec {

  "Splice Machine Connector" should "support batch reading" in
    withSparkSession { spark =>
      val q = spark
        .read
        .format("spliceV2")
        .load
      val leaves = q.queryExecution.logical.collectLeaves()
      leaves should have length 1

      import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
      import org.apache.spark.sql.sources.DataSourceRegister
      val source = leaves
        .head
        .asInstanceOf[DataSourceV2Relation]
        .source
        .asInstanceOf[DataSourceRegister]
      source.shortName() should be(SpliceDataSourceV2.NAME)
    }

  it should "support streaming write (with extra session-scoped options)" in
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
        .format("spliceV2")
        .option("splice.option", "option-value")
        .option("checkpointLocation", s"target/checkpointLocation-${UUID.randomUUID()}")
        .trigger(Trigger.ProcessingTime(1.second))
        .start()

      sq.isActive should be(true)

      // FIXME Let the streaming query execute twice or three times exactly
      import concurrent.duration._
      sq.awaitTermination(2.seconds.toMillis)
      sq.stop()

      sq.isActive should be(false)

      val progress = sq.lastProgress
      progress.sink.description should be("splice.v2.SpliceDataSourceV2[spliceV2]")
    }

}
