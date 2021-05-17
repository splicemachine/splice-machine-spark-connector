package com.spicemachine.spark.ingester

import java.util.Properties
import java.util.concurrent.{BlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.log4j.Logger
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.spark.sql.types.StructType
import com.splicemachine.spark2.splicemachine.SplicemachineContext
import com.splicemachine.spark2.splicemachine.SplicemachineContext.RowForKafka

class Inserter(
    id: String,
    spliceUrl: String, 
    spliceKafkaServers: String, 
    spliceKafkaPartitions: String,
    useFlowMarkers: Boolean,
    spliceTable: String, 
    dfSchema: StructType,
    taskQueue: BlockingQueue[(Seq[RowForKafka], Long, String)],
    batchCountQueue: BlockingQueue[Long],
    processing: AtomicBoolean,
    loggingOn: Boolean = false
  )
  extends Runnable {

  private val logger = Logger.getLogger(getClass.getName)
  
  val nsds = new SplicemachineContext(Map(
    "url" -> spliceUrl,
    "KAFKA_SERVERS" -> spliceKafkaServers,
    "KAFKA_TOPIC_PARTITIONS" -> spliceKafkaPartitions,
    "USE_FLOW_MARKERS" -> useFlowMarkers.toString
  ))
  
  nsds.setTable(spliceTable, dfSchema)

  val log: String => Unit = (s: String) => if (loggingOn) { logger.info(s) }

  val props = new Properties
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, spliceKafkaServers)
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "spark-producer-ssds-metrics-"+java.util.UUID.randomUUID() )
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer].getName)

  val metricsProducer = new KafkaProducer[Integer, Long](props)
  val metricsTopic = "ssds-metrics"
  
  override def run(): Unit =
    do {
      try {
        // From a LinkedBlockingQueue, get object containing topicname and rcd count
        val task = taskQueue.poll(100L, TimeUnit.MILLISECONDS)
        if (task != null && task._2 > 0) {
          val lastRows = task._1
          val batchCount = task._2
          val topicInfo = task._3
          //lastRows.foreach( _.send(true) )
          //lastRows.headOption.foreach( _.close )
          //log(s"${java.time.Instant.now} $id INS last rows ${lastRows.mkString("\n")}")
          nsds.sendData(lastRows, true)
          if (lastRows.headOption.isDefined) {
            //val topicName = lastRows.head.topicName
            log(s"$id INS task $topicInfo $batchCount")
            // Call NSDS insert
            nsds.insert_streaming(topicInfo)
            log(s"$id INS inserted")
            // Send rcd count to metrics topic
            metricsProducer.send(new ProducerRecord(
              metricsTopic,
              batchCount
            ))
            //batchCountQueue.put(batchCount)
            log(s"$id INS metrics sent")
          } else {
            logger.error(s"$id INS ERROR topic name not found")
          }
        } else if (task != null && task._2 == 0) {
          log(s"$id INS no recs")
        }
      } catch {
        case e: Throwable =>
          logger.error(s"$id INS ERROR Insert failed\n$e\n${e.getMessage}")
//          e.printStackTrace
      }
    } while(processing.get)
}
