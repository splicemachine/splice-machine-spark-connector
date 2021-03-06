import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{BlockingDeque, BlockingQueue, TimeUnit}

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
              conserveTopics: Boolean = false
  )
  extends Runnable {

  val nsds = new SplicemachineContext(Map(
    "url" -> spliceUrl,
    "KAFKA_SERVERS" -> spliceKafkaServers,
    "KAFKA_TOPIC_PARTITIONS" -> spliceKafkaPartitions,
    "USE_FLOW_MARKERS" -> useFlowMarkers.toString
  ))
  
//  val s0 = Seq.empty[String]

  override def run(): Unit =
    do {
      try {
        println(s"${java.time.Instant.now} $id LOAD check for data")
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
            println(s"${java.time.Instant.now} $id LOAD WARN topic name not found")
            newTopic = true
            (nsds.newTopic_streaming(), 0L)
          }
        }

        println(s"${java.time.Instant.now} $id LOAD to $topicName")
        val (lastRows: Seq[RowForKafka], batchCount: Long, ptnInfo: Array[String]) = nsds.sendData_streaming(df, topicName)
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
        
        println(s"${java.time.Instant.now} $id LOAD batch count $newTotal")
        taskQueue.put((lastRows, newTotal, topicInfo))
      } catch {
        case e: Throwable =>
          println(s"${java.time.Instant.now} $id LOAD ERROR Load failed")
          e.printStackTrace
      }
    } while(processing.get)

}
