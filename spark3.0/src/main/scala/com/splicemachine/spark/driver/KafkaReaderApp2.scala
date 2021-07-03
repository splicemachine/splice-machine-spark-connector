package com.splicemachine.spark.driver

import java.util.Properties
import java.util.concurrent.{LinkedBlockingDeque, LinkedTransferQueue}
import java.util.concurrent.atomic.AtomicBoolean
import java.sql.{Connection, DriverManager, PreparedStatement, SQLException, Timestamp}

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
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.Logger
import com.spicemachine.spark.ingester.SLIIngester
import com.spicemachine.spark.ingester.component.LoadedTimestampTracker
import com.spicemachine.spark.ingester.component.InsertedTimestampTracker

case class InputData(fullTagName: String, tagName: String, time: Timestamp, value: String, quality: String)
case class DeltaData(endTime: Timestamp, delta: Long, value: String, quality: String, valueState: String)
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
    val checkpointLocationRootDir = args.slice(11,12).headOption.getOrElse("/tmp")
    val upsert = args.slice(12,13).headOption.getOrElse("false").toBoolean
    val conserveTopics = args.slice(13,14).headOption.getOrElse("true").toBoolean
    val jdbcMode = args.slice(14,15).headOption.getOrElse("true").toBoolean
    val groupId = args.slice(15,16).headOption.getOrElse("")
    val clientId = args.slice(16,17).headOption.getOrElse("")
    val eventFormat = args.slice(17,18).headOption.getOrElse("flat")
    val dataTransformation = args.slice(18,19).headOption.getOrElse("false").toBoolean
    val tagFilename = args.slice(19,20).headOption.getOrElse("")
    val useFlowMarkers = args.slice(20,21).headOption.getOrElse("false").toBoolean
    val maxPollRecs = args.slice(21,22).headOption

    val log = Logger.getLogger(getClass.getName)

    val spark = SparkSession.builder.appName(appName).getOrCreate()
    import spark.implicits._

    val spliceTsFormatter = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")

    val lastVals = {
      val splice = new SplicemachineContext(Map(
        "url" -> spliceUrl,
        "KAFKA_SERVERS" -> spliceKafkaServers,
        "KAFKA_TOPIC_PARTITIONS" -> spliceKafkaPartitions
      ))
//      val tagLookup = splice.df("select * from OCI.TAGLOOKUPWITHIDENTITY").select("FULLTAGNAME", "TAG_TIMEOUT_SECONDS")
      
      // {"TAG": "OCIB.Kep.VYI-4164.PV", "SERVERTIME": "2021-06-25 18:29:47.961302", 
      //  "SOURCETIME": "2021-06-25 18:29:47.961302", "VALUE": 0.6084445714950562, "STATUS": "StatusCode(Good)"}
      //
      // InputData(fullTagName: String, tagName: String, time: Timestamp, value: String, quality: String)

//      val allTags = splice.df("select CONVERTED_FULL_TAG_NAME from OCI2.KEP_TAG_CONVERSION where CONVERTED_FULL_TAG_NAME IS NOT NULL")
//        .distinct.map(_.getAs[String]("CONVERTED_FULL_TAG_NAME")).collect
////        .map(r => new InputData( r.getAs[String]("CONVERTED_FULL_TAG_NAME"), "DUMMY",
////          new Timestamp(System.currentTimeMillis), "0.0", "0" )
////        )
////        .groupByKey(input => input.fullTagName)

      log.info("Getting previous values of tags")
//TIME_WEIGHTED_VALUE, VALUE_STATE, QUALITY,
      val lastVal = collection.mutable.Map.empty[String,(Double,String,Int)]
      splice.df("""select out_table.* from --splice-properties joinOrder=fixed
                  |(
                  |   select full_tag_name, max(start_ts) as max_ts from (
                  |    select rs.full_tag_name, start_ts from --splice-properties joinOrder=fixed 
                  |       oci2.kep_tag_conversion taglist,
                  |       oci2.resampled_data_1m rs --splice-properties joinStrategy=nestedloop
                  |    where start_ts > current_timestamp - 3 days
                  |    and rs.full_tag_name = taglist.converted_full_tag_name
                  |  )
                  |  group by full_tag_name 
                  |  order by full_tag_name, max_ts ) in_table  --splice-properties joinStrategy=nestedloop
                  |, oci2.resampled_data_1m as out_table --splice-properties joinStrategy=nestedloop
                  |where in_table.full_tag_name = out_table.full_tag_name and in_table.max_ts = out_table.start_ts""".stripMargin)
          .collect
          .foreach(r => {
            if(r.isNullAt(0) || r.isNullAt(1) || r.isNullAt(2) || r.isNullAt(3) || r.isNullAt(4) || r.isNullAt(5)) {
              val msg = s"${r.getAs[String]("FULL_TAG_NAME")} will be dropped, its record at ${r.getAs[Timestamp]("START_TS")} contains NULL"
              log.warn(msg)
              println(msg)
            } else {
              lastVal += r.getAs[String]("FULL_TAG_NAME") -> (r.getAs[Double]("TIME_WEIGHTED_VALUE"), r.getAs[String]("VALUE_STATE"), r.getAs[Int]("QUALITY"))
            }
          })
      
//      splice.df("""select FULL_TAG_NAME, max(start_ts) from oci2.resampled_data_1m rs, oci2.kep_tag_conversion taglist
//           where start_ts > current_timestamp - 3 days
//           and rs.full_tag_name = taglist.converted_full_tag_name
//           and rs.quality is not null
//           group by FULL_TAG_NAME""")
//        .collect
//        .foreach(tagts => {
//          val v = splice.df(
//            s"""select FULL_TAG_NAME, TIME_WEIGHTED_VALUE, VALUE_STATE, QUALITY from OCI2.RESAMPLED_DATA_1M
//               | where FULL_TAG_NAME='${tagts.getString(0)}'
//               | and START_TS='${spliceTsFormatter.format(tagts.getTimestamp(1)).toString}'
//               |""".stripMargin
//          ).map(r => (r.getString(0), r.getDouble(1), r.getString(2), r.getInt(3))).reduce((r1, r2) => r1)
//          lastVal += v._1 -> (v._2, v._3, v._4)
//        })
        
//        .map(r => (r.getString(0), r.getDouble(1), r.getString(2), r.getInt(3)))
//        .collect
//        .foreach(d => lastVal += d._1 -> (d._2, d._3, d._4))

//      allTags.foreach(tag => {
//        val maxts = splice.df(
//          s"""select max(start_ts) from OCI2.RESAMPLED_DATA_1M
//             | where start_ts > current_timestamp - 3 days
//             | and FULL_TAG_NAME = '$tag'
//             |""".stripMargin).collect
//        if( ! maxts(0).isNullAt(0) ) {
//          val df = splice.df(
//            s"""select FULL_TAG_NAME, TIME_WEIGHTED_VALUE, VALUE_STATE, QUALITY from OCI2.RESAMPLED_DATA_1M
//               | where FULL_TAG_NAME='$tag'
//               | and START_TS='${spliceTsFormatter.format(maxts(0).getTimestamp(0)).toString}'
//               |""".stripMargin)
//          val v = df.map(r => (r.getString(0), r.getDouble(1), r.getString(2), r.getInt(3))).reduce((r1, r2) => r1)
//          lastVal += v._1 -> (v._2, v._3, v._4)
//        }
//      })
      lastVal
    }

    def parseCheckpointLocation(pathValue: String): String = {
      var validCheckPointLocation = "/tmp/"
      val configuration = new org.apache.hadoop.conf.Configuration();
      val pathValues = pathValue.split(";")
      var found = false
      var i = 0
      while (!found && i < pathValues.size) {
        val value = pathValues(i)
        log.info(s"Checking NN $value")
        i = i + 1
        try {
          val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(value), configuration);
          if (hdfs.exists(new org.apache.hadoop.fs.Path(new java.net.URI(value)))) {
            validCheckPointLocation = value
            found = true
            log.info(s"Found NN $value")
          }
        } catch {
          case e: Throwable => {
            log.warn(s"NN $value is not available: ${e.getMessage}")
          }
        }
      }
      if(!found) {
        log.warn(s"Can't find a valid checkpoint location from input param: $pathValue")
      }
      if(!validCheckPointLocation.endsWith("/")) { validCheckPointLocation+"/" } else {validCheckPointLocation}
    }

    val chkpntRoot = parseCheckpointLocation(checkpointLocationRootDir)

//    val chkpntRoot = if(!checkpointLocationRootDir.endsWith("/")) { checkpointLocationRootDir+"/" } else {checkpointLocationRootDir}

    log.info(s"Checkpoint Location: $chkpntRoot")
    
    // Recommended when using stateful stream queries, based on
    //  https://docs.databricks.com/spark/latest/structured-streaming/production.html#optimize-performance-of-stateful-streaming-queries
//    spark.conf.set(
//      "spark.sql.streaming.stateStore.providerClass",
//      "org.apache.spark.sql.execution.streaming.state.RocksDbStateStoreProvider"
//      // pass com.qubole.spark:spark-rocksdb-state-store_2.11:1.0.0 in the packages param in spark-submit
//    )
    
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
    
    val kafkaProducerProps = new Properties
    kafkaProducerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, spliceKafkaServers)
    kafkaProducerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName)
    kafkaProducerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    val resampledEventTopic = "ResampledEvent"
    
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
    val windowMs = windowSize * 1000
    val watermarkThreshold = 2 * windowSize
    val watermarkThresholdUnits = windowSizeUnits
    val watermarkThresholdMs = watermarkThreshold * 1000

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
    
    def twa(wnData: Seq[DeltaData]): Double = wnData.map(d => {
      try {
        d.delta * d.value.toDouble / windowMs
      } catch {
        case e: Throwable => 0
      }
    }).sum
    
    def wnQuality(wnData: Seq[DeltaData]): Int = wnData
      .map(d => try{ d.quality.toInt } catch {
        case e: Throwable => if(d.quality.toString.contains("StatusCode(Good)")) {192} else {0}
      }).min
    
    def wnState(wnData: Seq[DeltaData]): String = if( wnData.map(_.valueState).contains("A") ) {"A"} else {"F"}

    def wnResults(tag: String, curWindow: (Timestamp,Timestamp), wnData: Seq[DeltaData]): ResultData =
      new ResultData(tag, curWindow._1, curWindow._2, twa(wnData), wnState(wnData), wnQuality(wnData))

    def timeWeightedAverage(df: Dataset[InputData]): DataFrame = df
      .groupByKey(input => input.fullTagName)
//      .groupBy(
//        window($"TIME", s"$windowSize $windowSizeUnits"),
//        $"FULLTAGNAME"
//      ) //.as[String,IncomingTail]
//      .cogroup(allTags)((tag: String, incomingItr: Iterator[InputData], allItr: Iterator[InputData]) => {
//        if( incomingItr.isEmpty ) {
//          allItr
//        } else {
//          incomingItr
//        }
//      })
//      .groupByKey(input => input.fullTagName)
      .flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.NoTimeout)((tag: String, valItr: Iterator[InputData], state: GroupState[Seq[InputData]]) => {
//      .flatMapGroups((tag,valItr) => {
        def sortByTime(r1: InputData, r2: InputData): Boolean = r1.time.before(r2.time)
        val newState = valItr.toSeq.filter(d => d.time != null).sortWith(sortByTime)
        //println(s"Count of $tag ${newState.size}")
        val itr = if(state.exists) {
          //println("State exists")
          val prevState = state.get.sortWith(sortByTime)
          val (wnStart, wnEnd) = windowOf(newState.head.time.getTime - watermarkThresholdMs)
          val (outOfScope, inScope) = prevState.span(_.time.before(wnStart))
//          if( outOfScope.size > 0 ) {
//            kafkaProducerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "spark-producer-ssds-resampling-"+java.util.UUID.randomUUID() )
//            val kafkaProducer = new KafkaProducer[Integer, String](kafkaProducerProps)
//            outOfScope.map(d => windowOf(d.time.getTime)._1.toString).distinct.foreach(t => kafkaProducer.send(new ProducerRecord(
//              resampledEventTopic,
//              s"$tag,${t}"
//            )))
//          }
          val inScopeFromWnStart = if(
            inScope.size == prevState.size
            || inScope.exists(! _.time.after(wnStart))
            || newState.exists(! _.time.after(wnStart)) )
          {
            inScope
          } else {
            val lastRecBeforeWnStart = prevState(prevState.size - inScope.size - 1)
            new InputData(lastRecBeforeWnStart.fullTagName, lastRecBeforeWnStart.tagName, wnStart,
              lastRecBeforeWnStart.value, lastRecBeforeWnStart.quality) +: inScope
          }
          val combinedState = (inScopeFromWnStart ++ newState).sortWith(sortByTime)
          state.update(combinedState)
          combinedState.iterator
        } else {
          //println(s"State didn't exist $newState ... ${newState.size}")
          if( newState.size > 0 ) { state.update(newState) }
          newState.iterator
        }
//         val itr = newState.iterator
        val res = Seq.newBuilder[ResultData]
        val wnData = Seq.newBuilder[DeltaData]
        if (itr.hasNext) {
          var prev = itr.next
          //println(prev)
          //println(prev.getClass.getName)
          var prevTime = prev.time.getTime
          var prevValue = prev.value
          var prevQuality = prev.quality
          var (nextWndEnd, nextWndStart) = nextWindowCut( prevTime )
          for (cur <- itr) {
            //println(cur)
            val curTime = cur.time.getTime
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
            prevValue = cur.value
            prevQuality = cur.quality
          }
          wnData += new DeltaData(new Timestamp(nextWndEnd), (nextWndStart-prevTime), prevValue, prevQuality, "F")
          res += wnResults(tag, windowOf(prevTime), wnData.result)
          wnData.clear
          //state.update(wnData)
        }
        //(tag, v.mkString("|"))
        //tag
        res.result.map(r => {
          val tag = if( r.fullTagName.contains("=") ) {
            val s = r.fullTagName.split("=")
            s(s.length - 1)
          } else { r.fullTagName }
          (tag, r.wndStart, r.wndEnd, r.twa, r.valueState, r.quality)
        }).iterator
//        wnData.result
      }).toDF("FULL_TAG_NAME","START_TS","END_TS","TIME_WEIGHTED_VALUE","VALUE_STATE","QUALITY")
    
//    (RowEncoder(schema), RowEncoder(dbSchema))

//    (RowEncoder(StructType(Seq(
//        StructField("state", StringType)))), RowEncoder(dbSchema))
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
    
    // {"TAG": "ns=2;s=BLOCK1.BLOCK1.RandomTag426", "SERVERTIME": "2021-06-17 15:45:59.105702", 
    //  "SOURCETIME": "2021-06-17 15:45:59.105702", "VALUE": 483, "STATUS": "StatusCode(Good)"}
    //
    //    {"FULLTAGNAME":"OCIB.Kep.FFIC731.PV", "TAGNAME":"FFIC731.PV", "TIME":
    //      􏰀"2021-01-01T00:02:45.000Z", "VALUE":0.0065188, "QUALITY":192}

    val values = if (useFlowMarkers) {
      reader
        .load.select(from_json(col("value") cast "string", schema, Map( "timestampFormat" -> "yyyy-MM-ddTHH:mm:ss.SSSZ" )) as "data", col("timestamp") cast "long" as "TM_EXT_KAFKA")
        .selectExpr("data.*", "TM_EXT_KAFKA * 1000 as TM_EXT_KAFKA" )
        .withColumn("TM_SSDS", unix_timestamp * 1000 )
    } else {
      reader
//        .load.select(from_json(col("value") cast "string", schema, Map( "timestampFormat" -> "yyyy-MM-ddTHH:mm:ss.SSSZ" )) as "data")
        .load.select(from_json(col("value") cast "string", schema, Map( "timestampFormat" -> "yyyy-MM-dd HH:mm:ss.SSSSSS" )) as "data")
        .select("data.*")
        .selectExpr("TAG as fullTagName", "TAG as tagName", "SOURCETIME as time", "VALUE as value", "STATUS as quality")
        .withWatermark("time", s"$watermarkThreshold $watermarkThresholdUnits")
        .as[InputData]
        .transform(timeWeightedAverage)  // ("FULL_TAG_NAME","START_TS","END_TS","TIME_WEIGHTED_VALUE","VALUE_STATE","QUALITY")
//        .coalesce(spliceKafkaPartitions.toInt)

//        .filter(r => {
//          var nonDouble = false
//          //try{ java.land.Double.valueOf( r.getAs[String]("value") ) } catch{ case e: Throwable => nonDouble = true }
//          try{ r.getAs[String]("value").toDouble } catch{ case e: Throwable => nonDouble = true }
//          nonDouble
//        }).select("value")

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
    
    val loadedQueue = new LinkedTransferQueue[(Timestamp,String)]()
    val insertedQueue = new LinkedTransferQueue[String]()
    
    var conn: Option[Connection] = None
    var insert: Option[PreparedStatement] = None
    var ingester: Option[SLIIngester] = None
    
    def setupJDBC(): Unit = {
      insert.foreach(ins => try{ ins.close } catch{case _ : Throwable => ;} )
      conn.foreach(con => try{ con.close } catch{case _ : Throwable => ;} )
      var connected = false
      var attempts = 0
      do {
        try {
          conn = Some(DriverManager.getConnection(spliceUrl))
          conn.get.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE)
          conn.get.setAutoCommit(false)
          val upsertStr = if (upsert) {
            " --splice-properties insertMode=UPSERT\n"
          } else {
            ""
          }
          insert = Some(conn.get.prepareStatement("INSERT INTO " + spliceTable + upsertStr + " VALUES (?, ?, ?, ?, ?, ?)"))
          connected = true
        } catch {
          case sqlExp: SQLException => {
            if( attempts % 60 == 0) {
              log.error(s"Problem setting up JDBC connection and prepared statement.\n$sqlExp")
              log.warn(s"Retrying JDBC setup")
            }
            Thread.sleep(1000)
          }
        }
        attempts += 1
      } while(!connected)
    }
    
    if(jdbcMode) {
      setupJDBC
    } else {
      log.info("Create SLIIngester")
      ingester = Some(new SLIIngester(
        numLoaders,
        numInserters,
        dbSchema,
        spliceUrl,
        spliceTable,
        spliceKafkaServers,
        spliceKafkaPartitions.toInt, // equal to number of partition in DataFrame
        Some(new LoadedTimestampTracker(loadedQueue, windowMs)),
        Some(new InsertedTimestampTracker(insertedQueue)),
        upsert,
        conserveTopics,
        true, // loggingOn: Boolean
        useFlowMarkers
      ))
    }

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

    val tsFormatter = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    var prevLastFinishWn = Timestamp.from(java.time.Instant.now)
//    var lastFinishWn = Timestamp.from(java.time.Instant.now)
//    var firstPublish = true
    val eventStart = if(eventFormat.equalsIgnoreCase("json")) { "{\"ResampledEvent\": \"" } else {""}
    val eventEnd = if(eventFormat.equalsIgnoreCase("json")) { "\"}" } else {""}
    //val ldMap = collection.mutable.Map.empty[String,Timestamp]

    var outState = collection.mutable.Map.empty[(String,Timestamp),(Timestamp,Double,String,Int)]
    val minutesInProcess = collection.mutable.Set.empty[Timestamp]

    val strQuery = values
      .writeStream
//      .outputMode("append")
      .option("checkpointLocation",s"${chkpntRoot}checkpointLocation-$spliceTable-${java.util.UUID.randomUUID()}")
//      .trigger(Trigger.ProcessingTime(s"$windowSize $windowSizeUnits"))
      .foreachBatch {
        (df: DataFrame, batchId: Long) => try {
          log.info(s"transfer next batch $batchId")
          
          val dfData = df.collect
          
//          val baselineState = outState.mapValues(v => {
//            (v._1, v._2, "F", v._4)
//          })
//          
//          baselineState.foreach(kv => {
//            outState += kv
//          })
          
          val activeMinutes = dfData.map(r => r.getAs[Timestamp]("START_TS")).distinct
          
          val completedMinutes = minutesInProcess.filter(m => ! activeMinutes.contains(m))
          
          completedMinutes.foreach(m => {
            outState = outState.filter((kv) => kv._1._2.after(m))
            minutesInProcess -= m
          })
          
          //val doubleNull: Double = Double.NaN
          activeMinutes.foreach(ts => {
            //val ts = r.getAs[Timestamp]("START_TS")
            if( ! minutesInProcess.contains(ts) ) {
              val end_ts = new Timestamp(ts.toInstant.plusSeconds(60).toEpochMilli)
              val prev_ts = new Timestamp(ts.toInstant.minusSeconds(60).toEpochMilli)
              lastVals.foreach(tagVal => {
                val tag = tagVal._1
                val v = tagVal._2
                //outState += ((tag,ts) -> (end_ts,doubleNull,"F",0))
                outState += (tag,ts) -> (if( outState.contains((tag,prev_ts)) ) {
                  val prevVal = outState((tag,prev_ts))
                  (end_ts, prevVal._2, "F", prevVal._4)
                } else {
                  (end_ts, v._1, "F", v._3)
                })
              })
              minutesInProcess += ts
            }
          })

          dfData.foreach(r => {
            outState += (r.getAs[String]("FULL_TAG_NAME"), r.getAs[Timestamp]("START_TS")) -> (
              r.getAs[Timestamp]("END_TS"), r.getAs[Double]("TIME_WEIGHTED_VALUE"), r.getAs[String]("VALUE_STATE"),
              r.getAs[Int]("QUALITY")
            )
          })
          
//          val batchDF = df.union(
//            Seq(("a",new Timestamp(1),new Timestamp(2),0.0,"F",0)).toDF("FULL_TAG_NAME","START_TS","END_TS","TIME_WEIGHTED_VALUE","VALUE_STATE","QUALITY")
//          ).coalesce(spliceKafkaPartitions.toInt)

          if (jdbcMode) {
            var retry = false
            do {
              try {
                retry = false
                var count = 0
                for ((k, v) <- outState) { //((kv) => {
                  val twa: java.lang.Double = if (v._2.isNaN) {
                    null
                  } else {
                    v._2
                  }
                  insert.get.setString(1, k._1)
                  insert.get.setTimestamp(2, k._2)
                  insert.get.setTimestamp(3, v._1)
                  insert.get.setDouble(4, twa)
                  insert.get.setString(5, v._3)
                  insert.get.setInt(6, v._4)
                  insert.get.addBatch()
                  count += 1
                  if (count == 1000) {
                    count = 0
                    insert.get.executeBatch
                    insert.get.clearBatch()
                  }
                }
                if (count > 0) {
                  insert.get.executeBatch()
                  insert.get.clearBatch()
                }
                conn.get.commit()
              } catch {
                case ntc: java.sql.SQLNonTransientConnectionException => {
                  log.error(s"Problem saving data to DB for batch $batchId.~\n$ntc")
                  retry = true
                }
                case sqlExp: SQLException => {
                  val exp = if (sqlExp.isInstanceOf[java.sql.BatchUpdateException]) { sqlExp.getNextException } else { sqlExp }
                  log.error(s"Problem saving data to DB for batch $batchId.-\n$exp")
                  if(
                    exp.toString.contains("RegionServerStoppedException")
                    || exp.toString.contains("RegionServerAbortedException")
                    || exp.toString.contains("Meta region is in state CLOSING")
                    || exp.toString.contains("Connection timed out")
                    || exp.toString.contains("connection has been terminated")
                    || exp.toString.contains("Connection reset")
                    || exp.toString.contains("Unable to fetch new timestamp")
                    || exp.toString.contains("timestamp source has been closed")
                    || exp.toString.contains("java.sql.SQLNonTransientConnectionException")
                  ) {
                    retry = true
                  }
                }
              }
              if(retry) {
                log.warn(s"Retrying DB call")
                Thread.sleep(1000)
                setupJDBC
              }
            } while(retry)
            completedMinutes.toSeq.sortWith((t1,t2) => t1.before(t2)).foreach(m => {
              kafkaProducerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "spark-producer-ssds-resampling-" + java.util.UUID.randomUUID())
              val kafkaProducer = new KafkaProducer[Integer, String](kafkaProducerProps)
              val tsStr = tsFormatter.format(m).toString
              kafkaProducer.send(new ProducerRecord(
                resampledEventTopic,
                s"$eventStart${tsStr}$eventEnd"
              ))
              kafkaProducer.flush
              kafkaProducer.close
              log.info(s"Published $tsStr after inserting batch $batchId")
            })
          }
          else {
            val insertData = outState.toSeq.map((kv) => {
              val k = kv._1
              val v = kv._2
              val twa: java.lang.Double = if( v._2.isNaN ) { null } else { v._2 }
              Row(k._1, k._2, v._1, twa, v._3, v._4)
              //          }).toDF("FULL_TAG_NAME","START_TS","END_TS","TIME_WEIGHTED_VALUE","VALUE_STATE","QUALITY")
            })

            val batchDF = spark.createDataFrame(
              spark.sparkContext.parallelize(insertData),
              StructType(Seq(
                StructField("FULL_TAG_NAME", StringType),
                StructField("START_TS", TimestampType),
                StructField("END_TS", TimestampType),
                StructField("TIME_WEIGHTED_VALUE", DoubleType),
                StructField("VALUE_STATE", StringType),
                StructField("QUALITY", IntegerType)
              ))
            ).coalesce(spliceKafkaPartitions.toInt)

            batchDF.persist
            batchDF.show(false)
            //log.info(s"Batch size: ${batchDF.count}")
            //batchDF.distinct.orderBy("value").show(false)

  //          ingester.ingest(batchDF.select(col("window") cast "string", col("FULLTAGNAME"), col("count")))
            ingester.get.ingest(batchDF)

            var count = 0
            var ldInfo = loadedQueue.peek
            while( ldInfo != null && count < 100 ) {
              val topic = ldInfo._2.split("::")(0)
              val ts = ldInfo._1
              if(insertedQueue.contains(topic)) {
                var sent = false
                while(!sent) {  // todo add counter to prevent inf loop
                  var insInfo = insertedQueue.poll
                  if (topic.equals(insInfo.split("::")(0))) {
                    //println(s"Publishing ${ts}")
                    kafkaProducerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "spark-producer-ssds-resampling-" + java.util.UUID.randomUUID())
                    val kafkaProducer = new KafkaProducer[Integer, String](kafkaProducerProps)
                    val tsStr = tsFormatter.format(ts).toString
                    kafkaProducer.send(new ProducerRecord(
                      resampledEventTopic,
                      s"$eventStart${tsStr}$eventEnd"
                    ))
                    kafkaProducer.flush
                    kafkaProducer.close
                    log.info(s"Published $tsStr after inserting $topic")
                    sent = true
                    loadedQueue.poll
  //                  minutesInProcess -= ts
  //                  outState = outState.filter((kv) => kv._1._2.after(ts))
                  }
                }
              } else {
                log.info(s"$topic not in insertedQueue: $insertedQueue")
              }
              //ldMap += ( topic -> ts )
              //println(s"Loaded $topic $ts $ldMap")
              ldInfo = loadedQueue.peek
              count += 1
            }

            batchDF.unpersist
          }
          
//          var insInfo = insertedQueue.poll
//          while( insInfo != null ) {
//            val topic = insInfo.split("::")(0)
//            println(s"Inserted $topic")
//            val ts = ldMap.remove(topic)
//            if (ts.isDefined) {
//              println(s"Publishing ${ts.get}")
//              kafkaProducerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "spark-producer-ssds-resampling-" + java.util.UUID.randomUUID())
//              val kafkaProducer = new KafkaProducer[Integer, String](kafkaProducerProps)
//              kafkaProducer.send(new ProducerRecord(
//                resampledEventTopic,
//                s"$eventStart${tsFormatter.format(ts.get).toString}$eventEnd"
//              ))
//            } //else {
//              //insertedQueue.put()
//            //}
//            insInfo = insertedQueue.poll
//          }
          
//          if(batchDF.count > 0) {
//            //println(lastFinishWn)
//            val minStart = batchDF.map(r => r.getAs[Timestamp]("START_TS"))
//              .reduce((t1, t2) => if (t1.before(t2)) {
//                t1
//              } else {
//                t2
//              })
//            lastFinishWn = windowOf(minStart.getTime-1)._1
//            //println(lastFinishWn)
//
//            if(!lastFinishWn.equals(prevLastFinishWn) && !firstPublish) {
//              kafkaProducerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "spark-producer-ssds-resampling-" + java.util.UUID.randomUUID())
//              val kafkaProducer = new KafkaProducer[Integer, String](kafkaProducerProps)
//              kafkaProducer.send(new ProducerRecord(
//                resampledEventTopic,
//                s"$eventStart${tsFormatter.format(lastFinishWn).toString}$eventEnd"
//              ))
//              prevLastFinishWn = lastFinishWn
//            } else {
//              firstPublish = false
//              prevLastFinishWn = lastFinishWn
//            }
//          }
          
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
    if(jdbcMode) {
      try {
        insert.get.close()
        conn.get.close()
      } catch {
        case sqlExp: SQLException => log.error(s"Problem closing JDBC connection.\n$sqlExp")
      }
    } else {
      ingester.get.stop()
    }
//    processing.compareAndSet(true, false)
  }
}
