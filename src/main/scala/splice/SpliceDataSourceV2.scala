package splice

import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

class SpliceDataSourceV2 extends DataSourceV2
  with DataSourceRegister
  with ReadSupport
  with StreamWriteSupport
  with SessionConfigSupport {

  override def shortName(): String = SpliceDataSourceV2.NAME

  override def createReader(options: DataSourceOptions): DataSourceReader = {
    new SpliceReader(options)
  }

  override def createStreamWriter(
    queryId: String,
    schema: StructType,
    mode: OutputMode,
    options: DataSourceOptions): StreamWriter = {
    new SpliceStreamWriter(queryId, schema, mode, options)
  }

  // Allows additional configuration layer beside user-defined options
  // Uses spark.datasource.splice options defined globally or per session
  override def keyPrefix(): String = SpliceDataSourceV2.NAME


  override def toString = s"${this.getClass.getCanonicalName}[$shortName]"
}

object SpliceDataSourceV2 {
  val NAME = "splice"
}
