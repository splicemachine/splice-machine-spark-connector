import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object KafkaReader {
  def main(args: Array[String]) {
    val appName = args(0)
    val externalKafkaServers = args(1)
    val externalTopic = args(2)
    val spliceUrl = args(3)
    val spliceTable = args(4)
    val spliceKafkaServers = args.slice(5,6).headOption.getOrElse("localhost:9092")
    val spliceKafkaTimeout = args.slice(6,7).headOption.getOrElse("20000")
    
    val spark = SparkSession.builder.appName(appName).getOrCreate()

    val schema = StructType(
      StructField("ID", StringType, false) ::
      StructField("LOCATION", StringType, true) ::
      StructField("TEMPERATURE", DoubleType, true) ::
      StructField("HUMIDITY", DoubleType, true) ::
      StructField("TM", TimestampType, true) :: Nil);

    val values = spark
      .readStream
      .format("kafka")
      .option("subscribe", externalTopic)
      .option("kafka.bootstrap.servers", externalKafkaServers)
      .load.select(from_json(col("value") cast "string", schema) as "data")
      .select("data.*")
    
    val strQuery = values
      .writeStream
      .option("checkpointLocation",s"/tmp/checkpointLocation-$spliceTable-${java.util.UUID.randomUUID()}")
      .foreachBatch {
        (batchDF: DataFrame, batchId: Long) => batchDF
          .write
          .format("splice")
          .option("url", spliceUrl)
          .option("table", spliceTable)
          .option("kafkaServers", spliceKafkaServers)
          .option("kafkaTimeout-milliseconds", spliceKafkaTimeout)
          .save
      }.start()

    strQuery.awaitTermination()
    strQuery.stop()
    spark.stop()
  }
}
