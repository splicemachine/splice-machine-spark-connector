import java.util.Properties
import java.util.concurrent.{LinkedBlockingDeque, LinkedTransferQueue}
import java.util.concurrent.atomic.AtomicBoolean

import concurrent.duration._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import com.splicemachine.spark2.splicemachine.SplicemachineContext
import com.splicemachine.spark2.splicemachine.SplicemachineContext.RowForKafka
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.LongSerializer

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
    val numLoaders = args.slice(7,8).headOption.getOrElse("1").toInt
    val numInserters = args.slice(8,9).headOption.getOrElse("1").toInt
    val maxPollRecs = args.slice(9,10).headOption
    val groupId = args.slice(10,11).headOption.getOrElse("")
    val clientId = args.slice(11,12).headOption.getOrElse("")

    val spark = SparkSession.builder.appName(appName).getOrCreate()

//    val props = new Properties
//    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, spliceKafkaServers)
//    props.put(ProducerConfig.CLIENT_ID_CONFIG, "spark-producer-ssds-metrics-"+java.util.UUID.randomUUID() )
//    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName)
//    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer].getName)
//
//    val metricsProducer = new KafkaProducer[Integer, Long](props)
//    val metricsTopic = "ssds-metrics"
    
    val schema = StructType(
      StructField("ID", StringType, false) ::
      StructField("LOCATION", StringType, true) ::
      StructField("TEMPERATURE", DoubleType, true) ::
      StructField("HUMIDITY", DoubleType, true) ::
      StructField("TM", TimestampType, true) :: Nil)

// Performance
//    val schema = StructType(
//      StructField("ID", LongType, false) ::
//      StructField("PAYLOAD", StringType, true) ::
//      StructField("SRC_SERVER", StringType, false) ::
//      StructField("SRC_THREAD", LongType, true) ::
//      StructField("TM_GENERATED", LongType, false) :: Nil)
////      StructField("PTN_NSDS", IntegerType, true) ::
////      StructField("TM_NSDS", LongType, true) :: Nil)
////      StructField("TM_EXT_KAFKA", LongType, true) :: Nil)

//    val schema = new SplicemachineContext(spliceUrl, externalKafkaServers).getSchema(spliceTable)

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
//    val inserter = new ParallelInsert
//    inserter.add(smc)
//    for(i <- 1 until numInserters) {
//      inserter.add(new SplicemachineContext(smcParams))
//    }

    val reader = spark
      .readStream
      .format("kafka")
      .option("subscribe", externalTopic)
      .option("kafka.bootstrap.servers", externalKafkaServers)
//      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss")
      //.option("minPartitions", minPartitions)  // probably better to rely on num partitions of the external topic
      .option("failOnDataLoss", "false")

    maxPollRecs.foreach( reader.option("kafka.max.poll.records", _) )

    if( ! groupId.isEmpty ) {
      val group = s"splice-ssds-$groupId"
      reader.option("kafka.group.id", group)
      reader.option("kafka.client.id", s"$group-$clientId")  // probably should use uuid instead of user input TODO
    }
    
    val values = reader
      .load.select(from_json(col("value") cast "string", schema, Map( "timestampFormat" -> "yyyy/MM/dd HH:mm:ss" )) as "data", col("timestamp") cast "long" as "TM_EXT_KAFKA")
      .select("data.*", "TM_EXT_KAFKA")
      .withColumn("TM_EXT_KAFKA", col("TM_EXT_KAFKA") * 1000)
      .withColumn("TM_SSDS", unix_timestamp * 1000 )
    
    val processing = new AtomicBoolean(true)
    val dataQueue = new LinkedTransferQueue[DataFrame]()
    val taskQueue = new LinkedBlockingDeque[(Seq[RowForKafka], Long, String)]()
    val batchCountQueue = new LinkedTransferQueue[Long]()

    val batchRegulation = new BatchRegulation(batchCountQueue)

    for(i <- 1 to numLoaders) {
      println(s"${java.time.Instant.now} create Loader L$i")
      new Thread(
        new Loader(
          "L" + i.toString,
          spliceUrl,
          spliceKafkaServers,
          spliceKafkaPartitions,
          dataQueue,
          taskQueue,
          batchRegulation,
          processing,
          true
        )
      ).start()
    }

    for(i <- 1 to numInserters) {
      println(s"${java.time.Instant.now} create Inserter I$i")
      new Thread(
        new Inserter(
          "I" + i.toString,
          spliceUrl,
          spliceKafkaServers,
          spliceKafkaPartitions,
          spliceTable,
          values.schema,
          taskQueue,
          batchCountQueue,
          processing
        )
      ).start()
      //    println(s"${java.time.Instant.now} insThr ${inserterThread.isAlive} ${inserterThread.getState}")
    }

//    for(i <- 1 to numInserters) {
//      new Thread(
//        new ParallelLoaderInserter(
//          "PLI" + i.toString,
//          spliceUrl,
//          spliceKafkaServers,
//          spliceKafkaPartitions,
//          spliceTable,
//          values.schema,
//          dataQueue,
//          processing
//        )
//      ).start
//    }
    
    val strQuery = values
      .writeStream
      .option("checkpointLocation",s"/tmp/checkpointLocation-$spliceTable-${java.util.UUID.randomUUID()}")
//      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss")
//      .trigger(Trigger.ProcessingTime(2.second))
      .foreachBatch {
        (batchDF: DataFrame, batchId: Long) => try {
          println(s"${java.time.Instant.now} transfer next batch")
          dataQueue.transfer(batchDF)
//          if( ! batchDF.isEmpty ) {
          
//          rcdCount = smc.insert(batchDF, spliceTable)  // these 6 lines were used up to Aug 23 2020
//          
//          metricsProducer.send( new ProducerRecord(
//            metricsTopic,
//            rcdCount
//          ))
          
/*          
//          var topic =
//            if( taskQueue.size > 1 ) {
//              val t = taskQueue.pollLast
//              if( t == null ) { s0 }
//              else if( t != null && taskQueue.size == 0 ) {
//                taskQueue.put(t) // don't take the last one, put it back
//                s0
//              } else t
//            } else s0
//
//          if( topic.isEmpty ) {
//            topic = Seq(smc.newTopic_streaming(), "0")
//          }
//
//          println(s"${java.time.Instant.now} insert to ${topic(0)}")
//
//          val batchCount = smc.sendData_streaming(batchDF, topic(0)) + topic(1).toLong
//          //if( taskQueue.size == 0 ) {
//          println(s"${java.time.Instant.now} batch count $batchCount")
//          taskQueue.put(Seq(topic(0), batchCount.toString))
*/
          
//            batchCount = 0L
//            topicName = smc.newTopic_streaming()
          //}
          
//          inserter.insert(batchDF, spliceTable) //
//          }
//          batchDF
//                    .write
//                    .format("splice")
//                    .option("url", spliceUrl)
//                    .option("table", spliceTable)
//                    .option("kafkaServers", spliceKafkaServers)
//                    //.option("kafkaTimeout-milliseconds", spliceKafkaTimeout)
//                    .save
          println(s"${java.time.Instant.now} transferred batch having ${batchDF.count}")  // todo log count as trace or diagnostic
        } catch {
          case e: Throwable =>
            println(s"${java.time.Instant.now} KafkaReader Exception processing batch $batchId having record count ${batchDF.count}")
            e.printStackTrace
        }
      }.start()

    strQuery.awaitTermination()
    strQuery.stop()
    spark.stop()
    processing.compareAndSet(true, false)
  }
}