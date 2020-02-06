package splice

import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SQLContext}

class SpliceSink(
    sqlContext: SQLContext,
    opts: SpliceOptions,
    partitionColumns: Seq[String],
    outputMode: OutputMode) extends Sink {
  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val url = opts.url
    val tableName = opts.table
    println(s">>> addBatch(batchId=$batchId) url=$url table=$tableName")
    data
      .write
      .format(SpliceDataSource.NAME)
      .option(SpliceOptions.JDBC_URL, url)
      .option(SpliceOptions.TABLE, tableName)
      .save

  }

  override def toString: String = s"${this.getClass.getCanonicalName}[${SpliceDataSource.NAME}]"
}
