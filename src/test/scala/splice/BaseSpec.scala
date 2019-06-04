package splice

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec, Ignore, Matchers}

@Ignore
class BaseSpec extends FlatSpec
  with Matchers
  with BeforeAndAfter {

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
