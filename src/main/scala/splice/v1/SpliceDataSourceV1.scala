package splice.v1

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider, SchemaRelationProvider, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

class SpliceDataSourceV1 extends SchemaRelationProvider
  with RelationProvider
  with DataSourceRegister
  with StreamSinkProvider {

  override def shortName(): String = SpliceDataSourceV1.NAME

  /**
    * For Spark SQL when no schema given explicitly
    */
  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String]): BaseRelation = {
    // FIXME Schema of the table we read from
    val schema = StructType(Seq(
      StructField("id", LongType),
      StructField("name", StringType)
    ))
    createRelation(sqlContext, parameters, schema)
  }

  /**
    * For Spark SQL with user-defined schema
    */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    val opts = new SpliceOptions(parameters)
    opts.assertRequiredOptionsDefined
    import com.splicemachine.spark.splicemachine.SplicemachineContext
    val spliceContext = new SplicemachineContext(opts.url)
    new SpliceRelation(schema, opts)(sqlContext.sparkSession)
  }

  /**
    * For Spark Structured Streaming
    */
  override def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {
    val opts = new SpliceOptions(parameters)
    new SpliceSink(sqlContext, opts, partitionColumns, outputMode)
  }
}

object SpliceDataSourceV1 {
  val NAME = "spliceV1"
}
