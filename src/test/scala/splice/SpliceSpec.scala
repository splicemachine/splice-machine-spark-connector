package splice

class SpliceSpec extends BaseSpec {

  "Splice Machine Connector" should "support batch reading" in
    withSparkSession { spark =>
      val q = spark
        .read
        .format("splice")
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

      import org.apache.spark.sql.streaming.Trigger
      import java.util.UUID
      val sq = spark
        .readStream
        .format("rate")
        .load
        .writeStream
        .format("splice")
        .option("splice.option", "option-value")
        .option("checkpointLocation", s"target/checkpointLocation-${UUID.randomUUID()}")
        .trigger(Trigger.Once())
        .start()

      sq.isActive should be (true)

      sq.processAllAvailable()
      sq.stop()

      sq.isActive should be (false)

      val progress = sq.lastProgress
      progress.sink.description should be ("splice.SpliceDataSourceV2[splice]")
    }

}
