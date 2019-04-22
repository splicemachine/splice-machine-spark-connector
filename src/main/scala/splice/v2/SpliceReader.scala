package splice.v2

import java.util.{List => JList}

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.types.{LongType, StructField, StructType, TimestampType}

class SpliceReader(options: DataSourceOptions) extends DataSourceReader {
  override def readSchema(): StructType = SpliceReader.SCHEMA

  override def createDataReaderFactories(): JList[DataReaderFactory[Row]] = ???
}

object SpliceReader {
  val SCHEMA = StructType(
    StructField("timestamp", TimestampType) ::
      StructField("value", LongType) ::
      Nil)
}
