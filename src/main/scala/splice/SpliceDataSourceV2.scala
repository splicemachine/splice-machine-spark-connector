package splice

import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}

class SpliceDataSourceV2 extends DataSourceV2
  with DataSourceRegister
  with ReadSupport {

  override def shortName(): String = SpliceDataSourceV2.NAME

  override def createReader(options: DataSourceOptions): DataSourceReader = {
    new SpliceReader(options)
  }

}

object SpliceDataSourceV2 {
  val NAME = "splice"
}
