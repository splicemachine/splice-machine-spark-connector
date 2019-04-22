package splice.v1

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import splice.BaseSpec

class SpliceDataSourceV1BatchSpec extends BaseSpec {

  "Splice Machine Connector (Data Source API V1 / Batch Mode)" should "support batch reading with explicit schema" in
    withSparkSession { spark =>
      val schema = StructType(Seq(
        StructField("id", LongType),
        StructField("name", StringType)
      ))
      val q = spark
        .read
        .format(SpliceDataSourceV1.NAME)
        .schema(schema)
        .option(SpliceOptions.JDBC_URL, "jdbc:splice://localhost:1527/splicedb")
        .option(SpliceOptions.USER, "FIXME_user")
        .option(SpliceOptions.PASSWORD, "FIXME_password")
        .load
      val leaves = q.queryExecution.logical.collectLeaves()
      leaves should have length 1

      import org.apache.spark.sql.execution.datasources.LogicalRelation
      leaves
        .head
        .asInstanceOf[LogicalRelation]
        .relation
        .asInstanceOf[SpliceRelation]
    }

  it should "throw an IllegalStateException when required options (e.g. url) are not defined" in
    withSparkSession { spark =>
      an [IllegalStateException] should be thrownBy {
        spark
          .read
          .format(SpliceDataSourceV1.NAME)
          .load
      }
    }
}
