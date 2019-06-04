package splice.v1

import com.splicemachine.spark.splicemachine.SplicemachineContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class SpliceDataSourceV1 extends SchemaRelationProvider
  with RelationProvider
  with DataSourceRegister
  with StreamSinkProvider
  with CreatableRelationProvider {

  override def shortName(): String = SpliceDataSourceV1.NAME

  /**
    * For Spark SQL when no schema given explicitly
    */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val opts = new SpliceOptions(parameters)
    val spliceCtx = new SplicemachineContext(opts.url)
    val schema = spliceCtx.getSchema(opts.table)
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
    new SpliceRelation(schema, opts)(sqlContext.sparkSession)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val spliceTable = createRelation(sqlContext, parameters, data.schema)
      .asInstanceOf[SpliceRelation]
    spliceTable.insert(data, overwrite = SaveMode.Overwrite == mode)
    spliceTable
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
