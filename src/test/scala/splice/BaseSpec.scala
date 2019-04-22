package splice

import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

class BaseSpec extends FlatSpec
  with Matchers {

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
