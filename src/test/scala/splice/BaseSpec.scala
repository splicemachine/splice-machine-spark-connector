package splice

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec, Ignore, Matchers}

@Ignore
class BaseSpec extends FlatSpec
  with Matchers
  with BeforeAndAfter {

  val tableName = this.getClass.getSimpleName
  val user = "splice"
  val password = "admin"
  val url = s"jdbc:splice://localhost:1527/splicedb;user=$user;password=$password"

  val sparkMaster = "local[*]"

  var spark: SparkSession = _

  before {
    spark = SparkSession
      .builder
      .master(sparkMaster)
      .appName(tableName)
      .config("spark.ui.enabled", false)
      .getOrCreate
  }

  def withSparkSession(testCode: SparkSession => Any): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("splice-test")
      .config("spark.ui.enabled", false)
      .getOrCreate
    try testCode(spark)
    finally spark.close
  }
}
