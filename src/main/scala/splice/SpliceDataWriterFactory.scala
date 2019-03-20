package splice

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory}

class SpliceDataWriterFactory extends DataWriterFactory[InternalRow] {
  override def createDataWriter(
    partitionId: Int,
    taskId: Long,
    epochId: Long): DataWriter[InternalRow] = new SpliceDataWriter
}
