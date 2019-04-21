package splice.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2.writer.{DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

class SpliceStreamWriter(
  queryId: String,
  schema: StructType,
  mode: OutputMode,
  options: DataSourceOptions) extends StreamWriter {

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    println(s">>> [SpliceStreamWriter.commit] epochId = $epochId | #messages = ${messages.length}")
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    println(s">>> [SpliceStreamWriter.abort] epochId = $epochId | #messages = ${messages.length}")
  }

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    println(s">>> [SpliceStreamWriter.createWriterFactory]")
    new SpliceDataWriterFactory(schema)
  }

  override def onDataWriterCommit(message: WriterCommitMessage): Unit = {
    println(s">>> SpliceStreamWriter.onDataWriterCommit($message)")
  }
}
