import java.util.Properties
import java.util.concurrent.{BlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.spark.sql.types.StructType
import com.splicemachine.spark2.splicemachine.SplicemachineContext
import com.splicemachine.spark2.splicemachine.SplicemachineContext.RowForKafka

class Inserter(
    spliceUrl: String, 
    spliceKafkaServers: String, 
    spliceKafkaPartitions: String,
    spliceTable: String, 
    dfSchema: StructType,
    taskQueue: BlockingQueue[(Seq[RowForKafka], Long)],
    batchCountQueue: BlockingQueue[Long],
    processing: AtomicBoolean
  )
  extends Runnable {
  
  val nsds = new SplicemachineContext(Map(
    "url" -> spliceUrl,
    "KAFKA_SERVERS" -> spliceKafkaServers,
    "KAFKA_TOPIC_PARTITIONS" -> spliceKafkaPartitions
  ))
  
  nsds.setTable(spliceTable, dfSchema)

  val props = new Properties
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, spliceKafkaServers)
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "spark-producer-ssds-metrics-"+java.util.UUID.randomUUID() )
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer].getName)

  val metricsProducer = new KafkaProducer[Integer, Long](props)
  val metricsTopic = "ssds-metrics"
  
  override def run(): Unit =
    do {
      // From a LinkedBlockingQueue, get object containing topicname and rcd count
      val task = taskQueue.poll(100L, TimeUnit.MILLISECONDS)
      if( task != null && task._2 > 0 ) {
        val lastRows = task._1
        val batchCount = task._2
        //lastRows.foreach( _.send(true) )
        //lastRows.headOption.foreach( _.close )
        //println(s"${java.time.Instant.now} INS last rows ${lastRows.mkString("\n")}")
        nsds.sendData(lastRows, true)
        if( lastRows.headOption.isDefined ) {
          val topicName = lastRows.head.topicName
          println(s"${java.time.Instant.now} INS task $topicName $batchCount")
          // Call NSDS insert
          nsds.insert_streaming(topicName)
          println(s"${java.time.Instant.now} INS inserted")
          // Send rcd count to metrics topic
          metricsProducer.send(new ProducerRecord(
            metricsTopic,
            batchCount
          ))
          batchCountQueue.put(batchCount)
          println(s"${java.time.Instant.now} INS metrics sent")
        } else {
          println(s"${java.time.Instant.now} INS ERROR topic name not found")
        }
      } else if( task != null && task._2 == 0 ) {
        println(s"${java.time.Instant.now} INS no recs")
      }
    } while(processing.get)
  
}
