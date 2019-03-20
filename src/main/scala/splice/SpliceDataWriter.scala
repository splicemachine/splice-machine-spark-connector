package splice

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}

class SpliceDataWriter extends DataWriter[InternalRow] {
  override def write(record: InternalRow): Unit = {
    println(s">>> [SpliceDataWriter.write] $record")
  }

  override def commit(): WriterCommitMessage = {
    println(s">>> [SpliceDataWriter.commit]")
    SpliceWriterCommitMessage("SpliceDataWriter.commit done")
  }

  override def abort(): Unit = {
    println(s">>> [SpliceDataWriter.abort]")
  }

}
