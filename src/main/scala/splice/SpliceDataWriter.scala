package splice

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

class SpliceDataWriter(schema: StructType) extends DataWriter[InternalRow] {
  override def write(record: InternalRow): Unit = {
    println(s">>> SpliceDataWriter.write(${record.toSeq(schema)})")
  }

  override def commit(): WriterCommitMessage = {
    println(s">>> [SpliceDataWriter.commit]")
    SpliceWriterCommitMessage("SpliceDataWriter.commit done")
  }

  override def abort(): Unit = {
    println(s">>> [SpliceDataWriter.abort]")
  }

}
