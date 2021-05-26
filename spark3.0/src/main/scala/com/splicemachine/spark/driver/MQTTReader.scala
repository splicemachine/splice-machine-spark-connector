package com.splicemachine.spark.driver

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import com.splicemachine.spark2.splicemachine.SplicemachineContext
import org.apache.log4j.Logger
import com.spicemachine.spark.ingester.SLIIngester

object MQTTReader {
  def main(args: Array[String]) {
    val appName = args(0)
    val externalMQTTServer = args(1)
    val externalTopic = args(2)
    val mqttClientId = args(3)
    var schemaDDL = args(4)
    val spliceUrl = args(5)
    val spliceTable = args(6)
    val spliceKafkaServers = args.slice(7,8).headOption.getOrElse("localhost:9092")
    val spliceKafkaPartitions = args.slice(8,9).headOption.getOrElse("1")
    val numLoaders = args.slice(9,10).headOption.getOrElse("1").toInt
    val numInserters = args.slice(10,11).headOption.getOrElse("1").toInt
    val useFlowMarkers = args.slice(11,12).headOption.getOrElse("false").toBoolean

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

    {
      val smc = new SplicemachineContext(
        Map(
          "url" -> spliceUrl,
          "KAFKA_SERVERS" -> spliceKafkaServers,
          "KAFKA_TOPIC_PARTITIONS" -> spliceKafkaPartitions
        )
      )

      if (!smc.tableExists(spliceTable)) {
        smc.createTable(spliceTable, schema)
      }
    }

    val values = spark
      .readStream
      .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
      .option("topic", "FD01")
      .option("clientId", "1")
      .option("persistence", "memory")
      .load("tcp://localhost:1883")
      .select($"payload" cast "string")

//    .select(from_json($"payload" cast "string", schema) as "data").select("data.*")
//      .select($"payload" cast "string").as[String]
    
//    val values = if (useFlowMarkers) {
//      reader
//        .load.select(from_json(col("value") cast "string", schema, Map( "timestampFormat" -> "yyyy/MM/dd HH:mm:ss" )) as "data", col("timestamp") cast "long" as "TM_EXT_KAFKA")
//        .selectExpr("data.*", "TM_EXT_KAFKA * 1000 as TM_EXT_KAFKA" )
//        .withColumn("TM_SSDS", unix_timestamp * 1000 )
//    } else {
//      reader
//        .load.select(from_json(col("value") cast "string", schema, Map( "timestampFormat" -> "yyyy/MM/dd HH:mm:ss" )) as "data")
//        .select("data.*")
//    }

    log.info("Create SLIIngester")
    val ingester = new SLIIngester(
      numLoaders,
      numInserters,
      values.schema,
      spliceUrl,
      spliceTable,
      spliceKafkaServers,
      spliceKafkaPartitions.toInt,  // equal to number of partition in DataFrame
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
  }
}
