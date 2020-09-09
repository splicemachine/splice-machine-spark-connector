import java.util.Properties
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.spark.sql.types.StructType
import com.splicemachine.spark2.splicemachine.SplicemachineContext

class Inserter(
    spliceUrl: String, 
    spliceKafkaServers: String, 
    spliceKafkaPartitions: String,
    spliceTable: String, 
    dfSchema: StructType,
    taskQueue: LinkedBlockingQueue[Seq[String]],
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
      if( task != null && task(1).toLong > 0 ) {
        println(s"${java.time.Instant.now} INS task $task")
        // Call NSDS insert
        nsds.insert_streaming(task(0))
        println(s"${java.time.Instant.now} INS inserted")
        // Send rcd count to metrics topic
        metricsProducer.send( new ProducerRecord(
          metricsTopic,
          task(1).toLong
        ))
        println(s"${java.time.Instant.now} INS metrics sent")
      } else if( task != null && task(1).toLong == 0 ) {
        println(s"${java.time.Instant.now} INS no recs")
      }
    } while(processing.get)
  
}
