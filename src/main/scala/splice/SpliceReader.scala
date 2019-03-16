package splice

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition}
import org.apache.spark.sql.types.{LongType, StructField, StructType, TimestampType}

class SpliceReader(options: DataSourceOptions) extends DataSourceReader {
  override def readSchema(): StructType = SpliceReader.SCHEMA

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = ???
}

object SpliceReader {
  val SCHEMA = StructType(
    StructField("timestamp", TimestampType) ::
      StructField("value", LongType) ::
      Nil)
}
