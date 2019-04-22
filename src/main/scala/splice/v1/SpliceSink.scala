package splice.v1

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode

class SpliceSink(
  sqlContext: SQLContext,
  opts: SpliceOptions,
  partitionColumns: Seq[String],
  outputMode: OutputMode) extends Sink {
  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    println(s">>> batchId=$batchId")
  }

  override def toString: String = s"${this.getClass.getCanonicalName}[${SpliceDataSourceV1.NAME}]"
}
