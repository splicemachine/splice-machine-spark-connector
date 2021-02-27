package splice

import java.util.UUID

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

class SpliceDataSourceBatchSpec extends BaseSpec {

  "Splice Machine Connector (Batch Mode)" should "support batch reading with explicit schema" in {
    val schema = StructType(Seq(
      StructField("ID", LongType),
      StructField("TEST_NAME", StringType)
    ))
    val q = spark
      .read
      .format(SpliceDataSource.NAME)
      .schema(schema)
      .option(SpliceOptions.JDBC_URL, url)
      .option(SpliceOptions.TABLE, tableName)
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

  it should "throw an IllegalStateException when required options (e.g. url) are not defined" in {
    an[IllegalStateException] should be thrownBy {
      spark
        .read
        .format(SpliceDataSource.NAME)
        .load
    }
  }

  it should "save a dataset to a table" in {
    val testName = this.getClass.getSimpleName
    val _spark = spark
    import _spark.implicits._
    // FIXME Splice supports dataframes with uppercase column names only
    val data = Seq((UUID.randomUUID().toString, testName)).toDF("ID", "TEST_NAME")
    data
      .write
      .format(SpliceDataSource.NAME)
      .option(SpliceOptions.JDBC_URL, url)
      .option(SpliceOptions.TABLE, tableName)
      .save
  }

  it should "read a dataset from a table" in {
    spark
      .read
      .format(SpliceDataSource.NAME)
      .option(SpliceOptions.JDBC_URL, url)
      .option(SpliceOptions.TABLE, tableName)
      .option("spark.sql.warehouse.dir", "target/spark-warehouse")
      .load
      .show(truncate = false)
  }
}
