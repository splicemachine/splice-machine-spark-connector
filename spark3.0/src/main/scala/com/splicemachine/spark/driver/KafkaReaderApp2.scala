package com.splicemachine.spark.driver

import java.util.Properties
import java.util.concurrent.{LinkedBlockingDeque, LinkedTransferQueue}
import java.util.concurrent.atomic.AtomicBoolean
import java.sql.Timestamp

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

case class DeltaData(endTime: Timestamp, delta: Long, value: Double, quality: Int, valueState: String)
//case class DeltaDataTail(endTime: Timestamp, delta: Long, value: Double, quality: Int, valueState: String, window: Window)
case class ResultData(fullTagName: String, wndStart: Timestamp, wndEnd: Timestamp, twa: Double, valueState: String, quality: Int)

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
    import spark.implicits._

    // Recommended when using stateful stream queries, based on
    //  https://docs.databricks.com/spark/latest/structured-streaming/production.html#optimize-performance-of-stateful-streaming-queries
    spark.conf.set(
      "spark.sql.streaming.stateStore.providerClass",
      "com.databricks.sql.streaming.state.RocksDBStateStoreProvider"
    )

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

//    val smcParams = Map(
//      "url" -> spliceUrl,
//      "KAFKA_SERVERS" -> spliceKafkaServers,
//      "KAFKA_TOPIC_PARTITIONS" -> spliceKafkaPartitions
//    )
//
//    val smc = new SplicemachineContext( smcParams )
//
//    if( ! smc.tableExists( spliceTable ) ) {
//      smc.createTable( spliceTable , schema )
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
    
    val dbSchema = StructType(Seq(
      StructField("FULL_TAG_NAME", StringType),
      StructField("START_TS", TimestampType),
      StructField("END_TS", TimestampType),
      StructField("TIME_WEIGHTED_VALUE", DoubleType),
      StructField("VALUE_STATE", StringType),
      StructField("QUALITY", IntegerType)
    ))

    val windowSize = 60
    val windowSizeUnits = "seconds" 
    val watermarkThreshold = 2 * windowSize
    val windowMs = windowSize * 1000

//    Input format:
//    {"FULLTAGNAME":"OCIB.Kep.FFIC731.PV", "TAGNAME":"FFIC731.PV", "TIME":
//      􏰀"2021-01-01T00:02:45.000Z", "VALUE":0.0065188, "QUALITY":192}
    
    def windowOf(ms: Long): (Timestamp,Timestamp) = {
      val wndStart = ms - (ms % windowMs)
      (new Timestamp(wndStart), new Timestamp(wndStart + windowMs))
    }

    def nextWindowCut(ms: Long): (Long, Long) = {
      val wndStart = ms + windowMs - (ms % windowMs)
      (wndStart - 1, wndStart)
    }
    
    def twa(wnData: Seq[DeltaData]): Double = wnData.map(d => d.delta * d.value / windowMs ).sum
    
    def wnQuality(wnData: Seq[DeltaData]): Int = wnData.map(_.quality).min
    
    def wnState(wnData: Seq[DeltaData]): String = if( wnData.map(_.valueState).contains("A") ) {"A"} else {"F"}

    def wnResults(tag: String, curWindow: (Timestamp,Timestamp), wnData: Seq[DeltaData]): ResultData =
      new ResultData(tag, curWindow._1, curWindow._2, twa(wnData), wnState(wnData), wnQuality(wnData))

    def xform(df: DataFrame): DataFrame = df
      .groupByKey(row => (row.getAs[String]("FULLTAGNAME")))
//      .groupBy(
//        window($"TIME", s"$windowSize $windowSizeUnits"),
//        $"FULLTAGNAME"
//      ) //.as[String,IncomingTail]
//      .flatMapGroupsWithState((tag,valItr,state) => {
      .flatMapGroups((tag,valItr) => {
        val sortedSeq = valItr.toSeq.sortWith((r1, r2) => r1.getTimestamp(2).before(r2.getTimestamp(2)))
        val itr = sortedSeq.iterator
        val res = Seq.newBuilder[ResultData]
        val wnData = Seq.newBuilder[DeltaData]
        if (itr.hasNext) {
          var prev = itr.next
          //println(prev)
          //println(prev.getClass.getName)
          var prevTime = prev.getAs[Timestamp]("TIME").getTime
          var prevValue = prev.getAs[Double]("VALUE")
          var prevQuality = prev.getAs[Int]("QUALITY")
          var (nextWndEnd, nextWndStart) = nextWindowCut( prevTime )
          for (cur <- itr) {
            //println(cur)
            val curTime = cur.getAs[Timestamp]("TIME").getTime
            while( curTime >= nextWndStart ) {
              wnData += new DeltaData(new Timestamp(nextWndEnd), (nextWndStart-prevTime), prevValue, prevQuality, "F")
              res += wnResults(tag, windowOf(prevTime), wnData.result)
              wnData.clear
              prevTime = nextWndStart
              val nextWndCut = nextWindowCut( nextWndStart )
              nextWndEnd = nextWndCut._1
              nextWndStart = nextWndCut._2
            }
            wnData += new DeltaData(new Timestamp(curTime), (curTime-prevTime), prevValue, prevQuality, "A")
            prevTime = curTime
            prevValue = cur.getAs[Double]("VALUE")
            prevQuality = cur.getAs[Int]("QUALITY")
          }
          wnData += new DeltaData(new Timestamp(nextWndEnd), (nextWndStart-prevTime), prevValue, prevQuality, "F")
          res += wnResults(tag, windowOf(prevTime), wnData.result)
          wnData.clear
          //state.update(wnData)
        }
        //(tag, v.mkString("|"))
        //tag
        res.result.map(r => Row(r.fullTagName, r.wndStart, r.wndEnd, r.twa, r.valueState, r.quality))
//        wnData.result
      }) (RowEncoder(dbSchema))
//      .toDF("FULL_TAG_NAME","START_TS","END_TS","TIME_WEIGHTED_VALUE","VALUE_STATE","QUALITY")
//      }).toDF("FULLTAGNAME","TIME","DELTA","VALUE","QUALITY","VALUE_STATE")
//    (RowEncoder(
//          StructType(Seq(
////            StructField("FULLTAG", StringType),
////            StructField("TAG", StringType),
////            StructField("TIME", TimestampType),
////            StructField("VALUE", DoubleType),
////            StructField("QUALITY", IntegerType)
//            StructField("EndTime", LongType),
//            StructField("Delta", LongType),
//            StructField("VALUE", DoubleType)
//          ))
//         ))
    //.toDF("fulltag", "tag", "time", "VALUE", "QUALITY")  //, "fields")

    val values = if (useFlowMarkers) {
      reader
        .load.select(from_json(col("value") cast "string", schema, Map( "timestampFormat" -> "yyyy-MM-ddTHH:mm:ss.SSSZ" )) as "data", col("timestamp") cast "long" as "TM_EXT_KAFKA")
        .selectExpr("data.*", "TM_EXT_KAFKA * 1000 as TM_EXT_KAFKA" )
        .withColumn("TM_SSDS", unix_timestamp * 1000 )
    } else {
      reader
        .load.select(from_json(col("value") cast "string", schema, Map( "timestampFormat" -> "yyyy-MM-ddTHH:mm:ss.SSSZ" )) as "data")
        .select("data.*")
        .withWatermark("TIME", s"$watermarkThreshold $windowSizeUnits")
        .transform(xform)
        .coalesce(spliceKafkaPartitions.toInt)
//        .groupBy(
//          window($"TIME", s"$windowSize $windowSizeUnits"),
//          $"FULLTAGNAME"
//        ).as[String,DeltaDataTail]
//        .mapGroups((tag,itr) => Row(tag)) (RowEncoder(
//          StructType(Seq(
//            StructField("FULLTAG", StringType)
//          ))
//        ))
    }

//    def process_batch(df, batchId): 
//    print(df.first()) 
//    valueRanges = df \
//    .orderBy(asc('time')) \
//      .rdd \
//    .map(lambda row: (row.FULLTAGNAME, (row.TIME, row.VALUE))) \ 
//    .reduceByKey(lambda v1,v2: list(v1)+list(v2)) \ 
//      .flatMapValues(toValueRanges) \
//    .map(lambda lst: Row(FULLTAGNAME=lst[0], TIME=lst[1][0], TDELTA=lst[1][1], VALUE=lst[1][2]))

    //# timeValues: (t1,v1,t2,v2,...)
    
    log.info("Create SLIIngester")

    val ingester = new SLIIngester(
      numLoaders,
      numInserters,
      dbSchema,
      spliceUrl,
      spliceTable,
      spliceKafkaServers,
      spliceKafkaPartitions.toInt,  // equal to number of partition in DataFrame
      upsert,
      true,  // loggingOn: Boolean
      useFlowMarkers
    )

//    def processBatch(df: DataFrame, batchId: Long): Unit = {
//      df
//        //        .orderBy(asc("TIME"))
//        //        .groupByKey(row => (row.getAs[String]("FULLTAGNAME")))
//        //        .map(row => (row.getAs[String]("FULLTAGNAME"), (row.getAs[Timestamp]("TIME"), row.getAs[Double]("VALUE"))))
//        //        .orderBy("window")
//        .show(false)
//      //df.filter(r => r.getAs[String]("VALUE_STATE").equals("F")).show(150, false)
//      ingester.ingest(df)
//    }

    val strQuery = values
      .writeStream
//      .outputMode("append")
      .option("checkpointLocation",s"/tmp/checkpointLocation-$spliceTable-${java.util.UUID.randomUUID()}")
//      .trigger(Trigger.ProcessingTime(s"$windowSize $windowSizeUnits"))
      .foreachBatch {
        (batchDF: DataFrame, batchId: Long) => try {
          log.info(s"transfer next batch")

//          batchDF.persist
//          batchDF.show(false)
          
//          ingester.ingest(batchDF.select(col("window") cast "string", col("FULLTAGNAME"), col("count")))
          ingester.ingest(batchDF)
          
//          processBatch(batchDF, batchId)
//          batchDF.unpersist

//          println(s"${java.time.Instant.now} transferred batch having ${batchDF.count}")  // todo log count as trace or diagnostic
          log.info(s"transferred batch")
        } catch {
          case e: Throwable =>
            log.error(s"KafkaReader Exception processing batch $batchId\n$e")
//            e.printStackTrace
        }
      }.start()

//    Thread.sleep(8000)
//    println(strQuery.lastProgress.json)

    strQuery.awaitTermination()
    strQuery.stop()
    spark.stop()
    ingester.stop()
//    processing.compareAndSet(true, false)
  }
}
