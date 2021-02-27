import java.time.LocalDateTime
import java.time.temporal.ChronoUnit.SECONDS
import java.util.{Collections, Properties, UUID}

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.mutable
import scala.collection.JavaConverters._

object KafkaOffsetCount {

  /**
    * @param args(0) kafkaServers
    * @param args(1) topicName
    */
  def main(args: Array[String]) {
    val ctr = new Counter(args(0), args(1))
    
    def now(): LocalDateTime = LocalDateTime.now

    val rateWindow = mutable.Queue.fill(args(2).toInt)(0.0)

    try {
      val t0 = now
      var ti0 = now
      var ti1 = now
      
      val c0 = ctr.offsetCount
      var ci0 = c0
      var ci1 = c0

      while(true) {
        Thread.sleep(1000)
        
        ci1 = ctr.offsetCount
        ti1 = now
        
        val curRate = (ci1-ci0).asInstanceOf[Double]/SECONDS.between(ti0,ti1)
        val ttlRate = (ci1-c0).asInstanceOf[Double]/SECONDS.between(t0,ti1)
        rateWindow.dequeue
        rateWindow.enqueue(curRate)

        println(s"\n$ti1")
        println(s"CurRate: $curRate  TtlRate: $ttlRate")
        println(
          rateWindow.min.round +" - "+
            (rateWindow.sum / rateWindow.size).round +" - "+
            rateWindow.max.round
        )

        ci0 = ci1
        ti0 = ti1
      }
    } catch {
      case e: Exception => {
        val msg = "KafkaOffsetCount: Problem processing." + "\n" + e.toString
        println(msg)
        throw e
      }
      case t: Throwable => {
        val msg = "KafkaOffsetCount: Problem processing." + "\n" + t.toString
        println(msg)
        throw t
      }
    } finally {
      ctr.close
    }
  }

  class Counter(kafkaServers: String, topicName: String) {
    var kConsumer: KafkaConsumer[String, String] = _
    var kTPartitions: mutable.Buffer[TopicPartition] = _

    {
      val props = new Properties()
      val groupId = "spark-consumer-ssds-kafkaoffsetcounter"
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers)
      props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
      props.put(ConsumerConfig.CLIENT_ID_CONFIG, groupId + "-" + UUID.randomUUID())
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000000")

      kConsumer = new KafkaConsumer[String, String](props)
      //    kConsumer.subscribe(util.Arrays.asList(topicName))  // for polling

      val partitionInfo = kConsumer.partitionsFor(topicName).asScala
      kTPartitions = partitionInfo.map(pi => new TopicPartition(topicName, pi.partition()))
      kConsumer.assign(kTPartitions.asJava)
    }

    def close(): Unit = {
      kConsumer.close
    }

    def offsetCount(): Long = { //kConsumer.poll(1000).count
      kConsumer.seekToEnd(Collections.emptySet())
      val endPartitions: Map[TopicPartition, Long] = kTPartitions.map(p => p -> kConsumer.position(p))(collection.breakOut)
      kConsumer.seekToBeginning(Collections.emptySet())
      kTPartitions.map(p => endPartitions(p) - kConsumer.position(p)).sum
      //    kTPartitions.map(p => kConsumer.position(p)).sum
    }
  }

//    {
////    var records = Iterable.empty[ConsumerRecord[Integer, Externalizable]]
//    var newRecords = consumer.poll(timeout).asScala // records: Iterable[ConsumerRecord[Integer, Externalizable]]
////    records = records ++ newRecords
//
////    while (newRecords.nonEmpty) {
////      newRecords = consumer.poll(shortTimeout).asScala // records: Iterable[ConsumerRecord[Integer, Externalizable]]
////      records = records ++ newRecords
////    }
//    consumer.close
//
//    newRecords.size
//  }

}
