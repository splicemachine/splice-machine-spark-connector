package splice.v1

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
    val spliceContext = new SplicemachineContext(opts.url)
    spliceContext.rdd(opts.table)
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val spliceContext = new SplicemachineContext(opts.url)
    spliceContext.insert(data, opts.table)
  }

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def toString: String = s"${this.getClass.getCanonicalName}[${SpliceDataSourceV1.NAME}]"
}
