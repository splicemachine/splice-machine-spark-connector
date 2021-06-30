package com.spicemachine.spark.ingester

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{LinkedBlockingDeque, LinkedTransferQueue}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.log4j.Logger

import com.spicemachine.spark.ingester.component.LoadAnalyzer
import com.spicemachine.spark.ingester.component.InsertCompleteAction

import com.splicemachine.spark2.splicemachine.SplicemachineContext.RowForKafka

class SLIIngester extends Ingester {  // Serial Loader Inserter == SLI

  private val log = Logger.getLogger(getClass.getName)
  
  private val processing = new AtomicBoolean(true)
  private val dataQueue = new LinkedTransferQueue[DataFrame]()

  def this(
    numLoaders: Int,    // 1
    numInserters: Int,  // 2
    schema: StructType,
    spliceDbUrl: String,
    spliceTableName: String,
    spliceKafkaServers: String,
    spliceKafkaPartitions: Int,  // equal to number of partition in DataFrame
    loadAnalyzer: Option[LoadAnalyzer],
    onInsertCompletion: Option[InsertCompleteAction],
    upsert: Boolean = false,
    conserveTopics: Boolean = true,
    loggingOn: Boolean = false,
    useFlowMarkers: Boolean = false  // for diagnostic use
  ) {
    this()
    log.info(s"Instantiate SLIIngester")

    val partitions = spliceKafkaPartitions.toString

    val taskQueue = new LinkedBlockingDeque[(Seq[RowForKafka], Long, String)]()
    val batchCountQueue = new LinkedTransferQueue[Long]()
    val batchRegulation = new BatchRegulation(batchCountQueue)

    for(i <- 1 to numLoaders) {
      log.info(s"Create Loader L$i")
      new Thread(
        new Loader(
          "L" + i.toString,
          spliceDbUrl,
          spliceKafkaServers,
          partitions,
          useFlowMarkers,
          dataQueue,
          taskQueue,
          loadAnalyzer,
          batchRegulation,
          processing,
          loggingOn,
          conserveTopics
        )
      ).start()
    }

    for(i <- 1 to numInserters) {
      log.info(s"Create Inserter I$i")
      new Thread(
        new Inserter(
          "I" + i.toString,
          spliceDbUrl,
          spliceKafkaServers,
          partitions,
          useFlowMarkers,
          spliceTableName,
          schema,
          upsert,
          taskQueue,
          batchCountQueue,
          onInsertCompletion,
          processing,
          loggingOn
        )
      ).start()
    }
  }
  
  def ingest(df: DataFrame): Unit = dataQueue.transfer(df)
  
  def stop(): Unit = processing.compareAndSet(true, false)
}
