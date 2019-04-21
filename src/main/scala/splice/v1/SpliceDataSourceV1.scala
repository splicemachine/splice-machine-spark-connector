package splice.v1

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

class SpliceDataSourceV1 extends RelationProvider
  with DataSourceRegister
  with StreamSinkProvider {

  override def shortName(): String = SpliceDataSourceV1.NAME

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    // FIXME Schema (either given or computed from the table we read from)
    val schema = StructType(Seq(
      StructField("id", LongType),
      StructField("name", StringType)
    ))
    new SpliceRelation(schema, parameters)(sqlContext.sparkSession)
  }

  override def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {
    new SpliceSink(sqlContext, parameters, partitionColumns, outputMode)
  }
}

object SpliceDataSourceV1 {
  val NAME = "spliceV1"
}
