package com.spicemachine.spark.ingester

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{BlockingDeque, BlockingQueue, TimeUnit}

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import com.splicemachine.spark2.splicemachine.SplicemachineContext
import com.splicemachine.spark2.splicemachine.SplicemachineContext.RowForKafka

class Loader(
              id: String,
              spliceUrl: String,
              spliceKafkaServers: String,
              spliceKafkaPartitions: String,
              useFlowMarkers: Boolean,
              dataQueue: BlockingQueue[DataFrame],
              taskQueue: BlockingDeque[(Seq[RowForKafka], Long, String)],
              batchRegulation: BatchRegulation,
              processing: AtomicBoolean,
              loggingOn: Boolean = false,
              conserveTopics: Boolean = false
  )
  extends Runnable {

  private val logger = Logger.getLogger(getClass.getName)

  val nsds = new SplicemachineContext(Map(
    "url" -> spliceUrl,
    "KAFKA_SERVERS" -> spliceKafkaServers,
    "KAFKA_TOPIC_PARTITIONS" -> spliceKafkaPartitions,
    "USE_FLOW_MARKERS" -> useFlowMarkers.toString
  ))

  val log: String => Unit = (s: String) => if (loggingOn) { logger.info(s) }
  
//  val s0 = Seq.empty[String]

  override def run(): Unit =
    do {
      try {
        log(s"$id LOAD check for data")
        val df = dataQueue.take()

        val task =
          if (conserveTopics && taskQueue.size > 1) {
            val t = taskQueue.pollLast()
            if (t == null) {
              None
            }
            else if ( /*batchRegulation.pass(t._2) &&*/ taskQueue.size > 0) {
              Some(t)
            } else {
              taskQueue.put(t)
              None
            }
          } else None

        var newTopic = false
        val (topicName: String, totalBatchCount) = if (task.isEmpty) {
          newTopic = true
          (nsds.newTopic_streaming(), 0L)
        } else {
          val lastRows = task.get._1
          if (lastRows.headOption.isDefined) {
            nsds.sendData(lastRows, false)
            (task.get._3, task.get._2)
          } else {
            logger.warn(s"$id LOAD WARN topic name not found")
            newTopic = true
            (nsds.newTopic_streaming(), 0L)
          }
        }

        log(s"$id LOAD to $topicName")
        val (lastRows: Seq[RowForKafka], batchCount: Long, ptnInfo: Array[String]) = try {
          nsds.sendData_streaming(df, topicName)
        } catch {
          case sparkExp: org.apache.spark.SparkException => {
            if( sparkExp.toString.contains("java.util.ConcurrentModificationException: KafkaConsumer is not safe for multi-threaded access") ) {
              Thread.sleep(1000)
              log(s"$id LOAD retry to $topicName")
              nsds.sendData_streaming(df, topicName)  // TODO support kafkaRecovery = true
            } else {
              throw sparkExp
            }
          }
        }
        val newTotal = totalBatchCount + batchCount
        
        val topicInfo: String = if( newTopic || topicName.contains("::") ) {
          val topicSuffix = nsds.topicSuffix(ptnInfo, spliceKafkaPartitions.toInt)
          if( newTopic || topicSuffix.isEmpty ) { topicName+topicSuffix }
          else {
            val activePartitions = Set.empty[String] ++ topicSuffix.split("::")(1).split(",") ++ topicName.split("::")(1).split(",")
            if( activePartitions.size == spliceKafkaPartitions.toInt ) { topicName }
            else { topicName+"::"+activePartitions.mkString(",") }
          }
        } else { topicName }
        
        log(s"$id LOAD batch count $newTotal")
        taskQueue.put((lastRows, newTotal, topicInfo))
      } catch {
        case e: Throwable =>
          logger.error(s"$id LOAD ERROR Load failed\n$e")
//          e.printStackTrace
      }
    } while(processing.get)

}
