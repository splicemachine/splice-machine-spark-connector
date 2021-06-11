package com.splicemachine.spark.driver

import java.util.Properties
import java.util.concurrent.{LinkedBlockingDeque, LinkedTransferQueue}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import com.splicemachine.spark2.splicemachine.SplicemachineContext
import com.splicemachine.spark2.splicemachine.SplicemachineContext.RowForKafka
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.log4j.Logger
import com.spicemachine.spark.ingester.SLIIngester

object KafkaReaderApp2 {
  def main(args: Array[String]) {
    val appName = args(0)
    val externalKafkaServers = args(1)
    val externalTopic = args(2)
    var schemaDDL = args(3)
    val spliceUrl = args(4)
    val spliceTable = args(5)
    val spliceKafkaServers = args.slice(6,7).headOption.getOrElse("localhost:9092")
    val spliceKafkaPartitions = args.slice(7,8).headOption.getOrElse("1")
//    val spliceKafkaTimeout = args.slice(7,8).headOption.getOrElse("20000")
    val numLoaders = args.slice(8,9).headOption.getOrElse("1").toInt
    val numInserters = args.slice(9,10).headOption.getOrElse("1").toInt
    val startingOffsets = args.slice(10,11).headOption.getOrElse("latest")
    val upsert = args.slice(11,12).headOption.getOrElse("false").toBoolean
    val dataTransformation = args.slice(12,13).headOption.getOrElse("false").toBoolean
    val tagFilename = args.slice(13,14).headOption.getOrElse("")
    val useFlowMarkers = args.slice(14,15).headOption.getOrElse("false").toBoolean
    val maxPollRecs = args.slice(15,16).headOption
    val groupId = args.slice(16,17).headOption.getOrElse("")
    val clientId = args.slice(17,18).headOption.getOrElse("")

    val log = Logger.getLogger(getClass.getName)

    val spark = SparkSession.builder.appName(appName).getOrCreate()

    // Create schema from ddl string like
    //    "ID STRING NOT NULL, LOCATION STRING, TEMPERATURE DOUBLE, HUMIDITY DOUBLE, TM TIMESTAMP"
    // or ".ID.LONG.NOT.NULL,PAYLOAD.STRING,SRC_SERVER.STRING.NOT.NULL,SRC_THREAD.LONG,TM_GENERATED.LONG.NOT.NULL"
    var schema = new StructType
    var splitter = " "
    var notNull = "NOT NULL"
    if( schemaDDL.trim.startsWith(".") ) {
      schemaDDL = schemaDDL.trim.substring(1, schemaDDL.length)
      splitter = "[.]"
      notNull = "NOT.NULL"
    }
    schemaDDL.split(",").foreach{ s =>
      val f = s.trim.split(splitter)
      schema = schema.add( f(0) , f(1) , ! s.toUpperCase.contains(notNull) )
    }
    log.info(s"schema: $schema")

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
    
    val reader = spark
      .readStream
      .format("kafka")
      .option("subscribe", externalTopic)
      .option("kafka.bootstrap.servers", externalKafkaServers)
      //.option("minPartitions", minPartitions)  // probably better to rely on num partitions of the external topic
      .option("failOnDataLoss", "false")
      .option("startingOffsets", startingOffsets)

    maxPollRecs.foreach( reader.option("kafka.max.poll.records", _) )

    if( ! groupId.isEmpty ) {
      val group = s"splice-ssds-$groupId"
      reader.option("kafka.group.id", group)
      reader.option("kafka.client.id", s"$group-$clientId")  // probably should use uuid instead of user input TODO
    }

    val values = if (useFlowMarkers) {
      reader
        .load.select(from_json(col("value") cast "string", schema, Map( "timestampFormat" -> "yyyy/MM/dd HH:mm:ss" )) as "data", col("timestamp") cast "long" as "TM_EXT_KAFKA")
        .selectExpr("data.*", "TM_EXT_KAFKA * 1000 as TM_EXT_KAFKA" )
        .withColumn("TM_SSDS", unix_timestamp * 1000 )
    } else {
      reader
        .load.select(col("value") cast "string")
    }

    log.info("Create SLIIngester")

    val ingester = new SLIIngester(
      numLoaders,
      numInserters,
      values.schema,
      spliceUrl,
      spliceTable,
      spliceKafkaServers,
      spliceKafkaPartitions.toInt,  // equal to number of partition in DataFrame
      upsert,
      true,  // loggingOn: Boolean
      useFlowMarkers
    )
    
    val strQuery = values
      .writeStream
      .option("checkpointLocation",s"/tmp/checkpointLocation-$spliceTable-${java.util.UUID.randomUUID()}")
//      .trigger(Trigger.ProcessingTime(2.second))
      .foreachBatch {
        (batchDF: DataFrame, batchId: Long) => try {
          log.info(s"transfer next batch")
          ingester.ingest(batchDF)
          
//          println(s"${java.time.Instant.now} transferred batch having ${batchDF.count}")  // todo log count as trace or diagnostic
          log.info(s"transferred batch")
        } catch {
          case e: Throwable =>
            log.error(s"KafkaReader Exception processing batch $batchId\n$e")
//            e.printStackTrace
        }
      }.start()

    strQuery.awaitTermination()
    strQuery.stop()
    spark.stop()
    ingester.stop()
//    processing.compareAndSet(true, false)
  }
}
