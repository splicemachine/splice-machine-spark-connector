package splice.v2

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class SpliceDataWriter(schema: StructType) extends DataWriter[Row] {
  override def write(record: Row): Unit = {
    println(s">>> SpliceDataWriter.write(${record.toSeq})")
  }

  override def commit(): WriterCommitMessage = {
    println(s">>> [SpliceDataWriter.commit]")
    SpliceWriterCommitMessage("SpliceDataWriter.commit done")
  }

  override def abort(): Unit = {
    println(s">>> [SpliceDataWriter.abort]")
  }

}
