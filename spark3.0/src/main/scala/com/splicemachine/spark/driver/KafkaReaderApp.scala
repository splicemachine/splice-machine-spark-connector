package com.splicemachine.spark.driver

import java.util.Properties
import java.util.concurrent.{LinkedBlockingDeque, LinkedTransferQueue}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
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

object KafkaReaderApp {
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

//    val props = new Properties
//    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, spliceKafkaServers)
//    props.put(ProducerConfig.CLIENT_ID_CONFIG, "spark-producer-ssds-metrics-"+java.util.UUID.randomUUID() )
//    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName)
//    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer].getName)
//
//    val metricsProducer = new KafkaProducer[Integer, Long](props)
//    val metricsTopic = "ssds-metrics"
    
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

//    val smc = new SplicemachineContext( smcParams )
//
//    if( ! smc.tableExists( spliceTable ) ) {
//      smc.createTable( spliceTable , schema )
//    }
    
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
      //.option("minPartitions", minPartitions)  // probably better to rely on num partitions of the external topic
      .option("failOnDataLoss", "false")
      .option("startingOffsets", startingOffsets)

    maxPollRecs.foreach( reader.option("kafka.max.poll.records", _) )

    if( ! groupId.isEmpty ) {
      val group = s"splice-ssds-$groupId"
      reader.option("kafka.group.id", group)
      reader.option("kafka.client.id", s"$group-$clientId")  // probably should use uuid instead of user input TODO
    }

    val tags = if( tagFilename.isEmpty ) { None } else { Some(
      spark.read.schema("i INT, TAG STRING, DUP STRING").csv(tagFilename)
    )}

    val formatter = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    
    // 2021-04-25T05:26:49.472+0300,2021-04-25T05:27:48.372,false,false,11.5,false,8,8.66,1124963504
    def toTags(df: DataFrame): DataFrame = if( dataTransformation && tags.isDefined ) {
      df.flatMap(row => {
        val data = row.getString(0).split(",")
        //val ts1 = data.head.replace("T"," ")
        //val ts = java.sql.Timestamp.valueOf( if( ts1.contains("+") ) { ts1.substring(0, ts1.indexOf("+")) } else { ts1 } )
        val parsedDate = formatter.parse(data.head, new java.text.ParsePosition(0))
        val ts = new java.sql.Timestamp( parsedDate.getTime() )
        data.slice(2, data.size).zipWithIndex.map(v => {  // skip 2nd timestamp, map the values
          val i = v._2 + 1
          val value: Option[Double] = try {
            v._1 match {
              case "false" => Some(0.0)
              case "true" => Some(1.0)
              case other => Some(other.toDouble)
            }
          } catch {
            case e: Throwable =>
              println( s"KafkaReader Problem processing incoming tag # ${i}\n$e" )
              None
          }
          Row( i, ts, value.getOrElse(null) )
        })
      }) (RowEncoder(StructType(Seq(
        StructField("i", IntegerType, false),
        StructField("RAWTIME", TimestampType, false),
        StructField("VALUE", DoubleType, true)
      ))))
        .join(tags.get, "i")
        .drop("i")
        .filter(r => r.getAs[String]("DUP") != "DUP")
        .drop("DUP")
    } else { df }

    val values = if (useFlowMarkers) {
      reader
        .load.select(from_json(col("value") cast "string", schema, Map( "timestampFormat" -> "yyyy/MM/dd HH:mm:ss" )) as "data", col("timestamp") cast "long" as "TM_EXT_KAFKA")
        .selectExpr("data.*", "TM_EXT_KAFKA * 1000 as TM_EXT_KAFKA" )
        .withColumn("TM_SSDS", unix_timestamp * 1000 )
    } else {
      reader
        .load.select(col("value") cast "string").transform(toTags)
    }

    //    val processing = new AtomicBoolean(true)

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
    
//    val dataQueue = new LinkedTransferQueue[DataFrame]()
//    val taskQueue = new LinkedBlockingDeque[(Seq[RowForKafka], Long, String)]()
//    val batchCountQueue = new LinkedTransferQueue[Long]()
//
//    val batchRegulation = new BatchRegulation(batchCountQueue)
//
//    for(i <- 1 to numLoaders) {   // ext
//      println(s"${java.time.Instant.now} create Loader L$i")
//      new Thread(
//        new Loader(
//          "L" + i.toString,       // int
//          spliceUrl,              // ext
//          spliceKafkaServers,     // ext
//          spliceKafkaPartitions,  // ext
//          useFlowMarkers,         // ext
//          dataQueue,              // int
//          taskQueue,              // int
//          batchRegulation,        // int
//          processing,             // ext
//          true                    // int
//        )
//      ).start()
//    }
//
//    for(i <- 1 to numInserters) { // ext
//      println(s"${java.time.Instant.now} create Inserter I$i")
//      new Thread(
//        new Inserter(
//          "I" + i.toString,       // int
//          spliceUrl,              // ext
//          spliceKafkaServers,     // ext
//          spliceKafkaPartitions,  // ext
//          useFlowMarkers,         // ext
//          spliceTable,            // ext
//          values.schema,          // ext
//          taskQueue,              // int
//          batchCountQueue,        // int
//          processing              // ext
//        )
//      ).start()
//      //    println(s"${java.time.Instant.now} insThr ${inserterThread.isAlive} ${inserterThread.getState}")
//    }

//    for(i <- 1 to numInserters) { // ext
//      new Thread(
//        new ParallelLoaderInserter(
//          "PLI" + i.toString,     // int     
//          spliceUrl,              // ext
//          spliceKafkaServers,     // ext
//          spliceKafkaPartitions,  // ext
//          useFlowMarkers,         // ext
//          spliceTable,            // ext
//          values.schema,          // ext
//          dataQueue,              // int
//          processing              // ext
//        )
//      ).start
//    }
    
    val strQuery = values
      .writeStream
      .option("checkpointLocation",s"/tmp/checkpointLocation-$spliceTable-${java.util.UUID.randomUUID()}")
//      .trigger(Trigger.ProcessingTime(2.second))
      .foreachBatch {
        (batchDF: DataFrame, batchId: Long) => try {
          log.info(s"transfer next batch")
//          dataQueue.transfer(batchDF)
          ingester.ingest(batchDF)
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
