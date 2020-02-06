package splice

import com.splicemachine.spark.splicemachine.SplicemachineContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

class SpliceRelation(
  override val schema: StructType,
  opts: SpliceOptions)(@transient val sparkSession: SparkSession)
  extends BaseRelation
    with PrunedFilteredScan
    with InsertableRelation {

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    println(">>> [SpliceRelation.buildScan] Registering the splice JDBC driver")
    // See https://github.com/jaceklaskowski/splice-machine-spark-connector/issues/14
    new com.splicemachine.db.jdbc.ClientDriver

    val spliceCtx = new SplicemachineContext(opts.url)
    // FIXME columnProjection is required
    spliceCtx.rdd(opts.table, requiredColumns)
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    println(">>> [SpliceRelation.insert] Registering the splice JDBC driver")
    // See https://github.com/jaceklaskowski/splice-machine-spark-connector/issues/14
    new com.splicemachine.db.jdbc.ClientDriver

    val spliceCtx = new SplicemachineContext(opts.url)
    val tableName = opts.table
    val isTableAvailable = spliceCtx.tableExists(tableName)
    if (!isTableAvailable) {
      spliceCtx.createTable(tableName, data.schema, keys = Seq.empty, createTableOptions = "UNUSED")
    } else if (isTableAvailable && overwrite) {
      spliceCtx.dropTable(tableName)
      spliceCtx.createTable(tableName, data.schema, keys = Seq.empty, createTableOptions = "UNUSED")
    }
    spliceCtx.insert(data, tableName)
  }

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def toString: String = s"${this.getClass.getCanonicalName}[${SpliceDataSource.NAME}]"
}
