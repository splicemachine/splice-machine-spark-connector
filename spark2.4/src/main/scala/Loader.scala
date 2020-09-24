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
              dataQueue: BlockingQueue[DataFrame],
              taskQueue: BlockingDeque[(Seq[RowForKafka], Long)],
              batchRegulation: BatchRegulation,
              processing: AtomicBoolean,
              conserveTopics: Boolean = false
  )
  extends Runnable {

  val nsds = new SplicemachineContext(Map(
    "url" -> spliceUrl,
    "KAFKA_SERVERS" -> spliceKafkaServers,
    "KAFKA_TOPIC_PARTITIONS" -> spliceKafkaPartitions
  ))
  
//  val s0 = Seq.empty[String]

  override def run(): Unit =
    do {
      println(s"${java.time.Instant.now} $id LOAD check for data")
      val df = dataQueue.take()

      val task =
        if (conserveTopics && taskQueue.size > 1) {
          val t = taskQueue.pollLast()
          if (t == null) {
            None
          }
          else if (batchRegulation.pass(t._2) && taskQueue.size > 0) {
            Some(t)
          } else {
            taskQueue.put(t)
            None
          }
        } else None

      val (topicName, totalBatchCount) = if (task.isEmpty) {
        (nsds.newTopic_streaming(), 0L)
      } else {
        val lastRows = task.get._1
        if (lastRows.headOption.isDefined) {
          nsds.sendData(lastRows)
          (lastRows.head.topicName, task.get._2)
        } else {
          println(s"${java.time.Instant.now} $id LOAD WARN topic name not found")
          (nsds.newTopic_streaming(), 0L)
        }
      }

      println(s"${java.time.Instant.now} $id LOAD to $topicName")
      val (lastRows: Seq[RowForKafka], batchCount) = nsds.sendData_streaming(df, topicName)
      val newTotal = totalBatchCount + batchCount

      println(s"${java.time.Instant.now} $id LOAD batch count $newTotal")
      taskQueue.put((lastRows, newTotal))

    } while(processing.get)

}
