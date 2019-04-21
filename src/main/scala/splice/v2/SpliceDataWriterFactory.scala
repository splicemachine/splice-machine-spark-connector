package splice.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType

class SpliceDataWriterFactory(schema: StructType) extends DataWriterFactory[InternalRow] {
  override def createDataWriter(
    partitionId: Int,
    taskId: Long,
    epochId: Long): DataWriter[InternalRow] = new SpliceDataWriter(schema)
}
