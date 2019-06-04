package splice.v1

import java.util.UUID

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import splice.BaseSpec

class SpliceDataSourceV1BatchSpec extends BaseSpec {

  val tableName = this.getClass.getSimpleName
  val user = "splice"
  val password = "admin"
  val url = s"jdbc:splice://localhost:1527/splicedb;user=$user;password=$password"

  var spark: SparkSession = _

  before {
    spark = SparkSession
      .builder
      .master("local[*]")
      .appName(tableName)
      .config("spark.ui.enabled", false)
      .getOrCreate
  }

  "Splice Machine Connector (Data Source API V1 / Batch Mode)" should "support batch reading with explicit schema" in {
    val schema = StructType(Seq(
      StructField("ID", LongType),
      StructField("TEST_NAME", StringType)
    ))
    val q = spark
      .read
      .format(SpliceDataSourceV1.NAME)
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
        .format(SpliceDataSourceV1.NAME)
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
      .format(SpliceDataSourceV1.NAME)
      .option(SpliceOptions.JDBC_URL, url)
      .option(SpliceOptions.TABLE, tableName)
      .save
  }

  it should "read a dataset from a table" in {
    spark
      .read
      .format(SpliceDataSourceV1.NAME)
      .option(SpliceOptions.JDBC_URL, url)
      .option(SpliceOptions.TABLE, tableName)
      .option("spark.sql.warehouse.dir", "target/spark-warehouse")
      .load
      .show(truncate = false)
  }
}
