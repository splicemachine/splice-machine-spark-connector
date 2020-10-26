package splice

import _root_.com.splicemachine.spark2.splicemachine.SplicemachineContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class SpliceDataSource extends SchemaRelationProvider
  with RelationProvider
  with DataSourceRegister
  with StreamSinkProvider
  with CreatableRelationProvider {

  override def shortName(): String = SpliceDataSource.NAME

  /**
    * For structured queries (Spark SQL) with no user-defined schema
    */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val opts = new SpliceOptions(parameters)
    val spliceCtx = getSplicemachineContext(opts)  // TODO splice
    val schema = spliceCtx.getSchema(opts.table)
    createRelation(sqlContext, parameters, schema)
  }

  /**
    * For structured queries (Spark SQL) with user-defined schema
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
    println(">>> createRelation")
    // FIXME Splice supports dataframes with uppercase column names only
    import org.apache.spark.sql.functions.col
    val columnNamesUpperCase = data.columns.map(_.toUpperCase).map(col).toSeq
    println(s">>> column names: $columnNamesUpperCase")
    val dataWithColumnNamesAllUpper = data.select(columnNamesUpperCase: _*)

    val spliceTable = createRelation(sqlContext, parameters, dataWithColumnNamesAllUpper.schema)
      .asInstanceOf[SpliceRelation]
    val overwrite = SaveMode.Overwrite == mode
    println(s">>> insert: $overwrite $dataWithColumnNamesAllUpper")
    spliceTable.insert(dataWithColumnNamesAllUpper, overwrite)
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

object SpliceDataSource {
  val NAME = "splice"
}
