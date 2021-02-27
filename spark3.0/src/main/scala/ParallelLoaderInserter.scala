import java.util.Properties
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{BlockingQueue, TimeUnit}

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import com.splicemachine.spark2.splicemachine.SplicemachineContext
import com.splicemachine.spark2.splicemachine.SplicemachineContext.RowForKafka

import scala.collection.JavaConverters._

class ParallelLoaderInserter(
    id: String,
    spliceUrl: String,
    spliceKafkaServers: String,
    spliceKafkaPartitions: String,
    spliceTable: String,
    dfSchema: StructType,
    dataQueue: BlockingQueue[DataFrame],
    processing: AtomicBoolean
  )
  extends Runnable {

  val nsds = new SplicemachineContext(Map(
    "url" -> spliceUrl,
    "KAFKA_SERVERS" -> spliceKafkaServers,
    "KAFKA_TOPIC_PARTITIONS" -> spliceKafkaPartitions
  ))

  val nsdsLoad = new SplicemachineContext(Map(
    "url" -> spliceUrl,
    "KAFKA_SERVERS" -> spliceKafkaServers,
    "KAFKA_TOPIC_PARTITIONS" -> spliceKafkaPartitions,
    "USE_FLOW_MARKERS" -> "true"
  ))

  val batchCount = new AtomicLong()
  var batchCountValue = 0L

  nsds.setTable(spliceTable, dfSchema)

  val props = new Properties
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, spliceKafkaServers)
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "spark-producer-ssds-metrics-"+java.util.UUID.randomUUID() )
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer].getName)

  val metricsProducer = new KafkaProducer[Integer, Long](props)
  val metricsTopic = "ssds-metrics"
  
//  private[this] val activePartitions =
//    SparkSession.builder.getOrCreate.sparkContext.collectionAccumulator[String]("ActivePartitions")

  override def run(): Unit =
    do {
      try {
        //println(s"${java.time.Instant.now} $id LDNS check for data")
        //val df = dataQueue.take
        val df = dataQueue.poll(100L, TimeUnit.MILLISECONDS)
        
        if( df != null ) {
          batchCount.compareAndSet(batchCountValue, 0L)
          batchCountValue = 0L

          val topicName = nsds.newTopic_streaming

//          activePartitions.reset
//          df.rdd.mapPartitionsWithIndex((p, itr) => {
//            activePartitions.add( s"$p, ${itr.nonEmpty}" )
//            Iterator.empty
//          }).collect

//          println(s"${java.time.Instant.now} $id LDNS checking active partitions")
//          val active = nsds.activePartitions(df)

          println(s"${java.time.Instant.now} $id LDNS starting Loaders")
          new Thread(
            new Loader(df, topicName, nsdsLoad)
          ).start

          println(s"${java.time.Instant.now} $id LDNS checking active partitions")
          val active = nsds.activePartitions(df)

          println(s"${java.time.Instant.now} $id LDNS task $topicName")
          // Call NSDS insert
          if( active.size == spliceKafkaPartitions.toInt ) {
            nsds.insert_streaming(topicName)
            println(s"${java.time.Instant.now} $id LDNS inserted")
          } else if( active.size > 0 ) {
            nsds.insert_streaming( s"$topicName::${active.mkString(",")}" )
            println(s"${java.time.Instant.now} $id LDNS inserted")
          }

          batchCountValue = batchCount.get
          while (batchCountValue == 0L) {
            Thread.sleep(100)
            batchCountValue = batchCount.get
          }

          if (batchCountValue > 0L) {
            // Send rcd count to metrics topic
            metricsProducer.send(new ProducerRecord(
              metricsTopic,
              batchCountValue
            ))
            //batchCountQueue.put(batchCount)
            println(s"${java.time.Instant.now} $id LDNS metrics sent")

            println(s"${java.time.Instant.now} $id LDNS batch count $batchCountValue")
            //taskQueue.put((lastRows, newTotal))
          } else {
            println(s"${java.time.Instant.now} $id LDNS no recs")
          }
        }
      } catch {
        case e: Throwable =>
          println(s"${java.time.Instant.now} $id LDNS ERROR Load failed")
          e.printStackTrace
      }
    } while(processing.get)

  
  class Loader(
    df: DataFrame,
    topicName: String,
    nsdsLoad: SplicemachineContext
  )
  extends Runnable {
    override def run(): Unit = {
      println(s"${java.time.Instant.now} $id LOAD to $topicName")
      val (lastRows: Seq[RowForKafka], recordCount: Long, ptnInfo: Array[String]) = nsdsLoad.sendData_streaming(df, topicName)
      if( lastRows.size > 0 ) { nsdsLoad.sendData(lastRows, true) }
      batchCount.compareAndSet(
        0L,
        if( recordCount == 0L ) {-1L} else recordCount
      )
    }

  }
}
