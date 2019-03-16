package splice

import org.apache.spark.sql.sources.DataSourceRegister

class SpliceSpec extends BaseSpec {

  "Splice Machine Connector" should "be registered as splice alias" in
    withSparkSession { spark =>
      val q = spark
        .read
        .format("splice")
        .load
      val leaves = q.queryExecution.logical.collectLeaves()
      leaves should have length 1

      import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
      val source = leaves
        .head
        .asInstanceOf[DataSourceV2Relation]
        .source
        .asInstanceOf[DataSourceRegister]
      source.shortName() should be(SpliceDataSourceV2.NAME)
    }

}
