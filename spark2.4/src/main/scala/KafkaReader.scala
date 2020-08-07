import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import com.splicemachine.spark2.splicemachine.SplicemachineContext

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object KafkaReader {
  def main(args: Array[String]) {
    val appName = args(0)
    val externalKafkaServers = args(1)
    val externalTopic = args(2)
    val spliceUrl = args(3)
    val spliceTable = args(4)
    val spliceKafkaPartitions = args.slice(5,6).headOption.getOrElse("1")
    val spliceKafkaServers = args.slice(6,7).headOption.getOrElse("localhost:9092")
//    val spliceKafkaTimeout = args.slice(7,8).headOption.getOrElse("20000")
    val groupId = args.slice(7,8).headOption.getOrElse("")
    val clientId = args.slice(8,9).headOption.getOrElse("")
    val numInserters = args.slice(9,10).headOption.getOrElse("1").toInt
    
    val spark = SparkSession.builder.appName(appName).getOrCreate()

//    val schema = StructType(
//      StructField("ID", StringType, false) ::
//      StructField("LOCATION", StringType, true) ::
//      StructField("TEMPERATURE", DoubleType, true) ::
//      StructField("HUMIDITY", DoubleType, true) ::
//      StructField("TM", TimestampType, true) :: Nil)

    val schema = StructType(
      StructField("ID", StringType, false) ::
      StructField("PAYLOAD", StringType, true) :: 
      StructField("TM", TimestampType, true) :: Nil)

    val smcParams = Map(
      "url" -> spliceUrl,
      "KAFKA_SERVERS" -> spliceKafkaServers,
      "KAFKA_TOPIC_PARTITIONS" -> spliceKafkaPartitions
    )

    val smc = new SplicemachineContext( smcParams )

    if( ! smc.tableExists( spliceTable ) ) {
      smc.createTable( spliceTable , schema )
    }
    
    // ParallelInsert didn't seem to help
    //  Usually run now with numInserters = 1
    val inserter = new ParallelInsert
    inserter.add(smc)
    for(i <- 1 until numInserters) {
      inserter.add(new SplicemachineContext(smcParams))
    }

    val reader = spark
      .readStream
      .format("kafka")
      .option("subscribe", externalTopic)
      .option("kafka.bootstrap.servers", externalKafkaServers)
      //.option("minPartitions", minPartitions)  // probably better to rely on num partitions of the external topic
      .option("failOnDataLoss", "false")
    
    if( ! groupId.isEmpty ) {
      val group = s"splice-ssds-$groupId"
      reader.option("kafka.group.id", group)
      reader.option("kafka.client.id", s"$group-$clientId")  // probably should use uuid instead of user input TODO
    }
    
    val values = reader
      .load.select(from_json(col("value") cast "string", schema) as "data")
      .select("data.*")
    
    val strQuery = values
      .writeStream
      .option("checkpointLocation",s"/tmp/checkpointLocation-$spliceTable-${java.util.UUID.randomUUID()}")
      .foreachBatch {
        (batchDF: DataFrame, batchId: Long) => {
          println(s"${(new java.util.Date).toString} insert")
//          if( ! batchDF.isEmpty ) {
          //smc.insert(batchDF, spliceTable)
          inserter.insert(batchDF, spliceTable)
//          }
          println(s"${(new java.util.Date).toString} insert inserted")
          //batchDF
          //          .write
          //          .format("splice")
          //          .option("url", spliceUrl)
          //          .option("table", spliceTable)
          //          .option("kafkaServers", spliceKafkaServers)
          //          .option("kafkaTimeout-milliseconds", spliceKafkaTimeout)
          //          .save
        }
      }.start()

    strQuery.awaitTermination()
    strQuery.stop()
    spark.stop()
  }
}
