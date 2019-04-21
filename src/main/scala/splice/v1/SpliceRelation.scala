package splice.v1

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType

class SpliceRelation(
    override val schema: StructType,
    parameters: Map[String, String])(@transient val sparkSession: SparkSession)
  extends BaseRelation
  with PrunedFilteredScan
  with InsertableRelation {

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    new SpliceRDD(sparkSession.sparkContext, requiredColumns, filters)
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = ???

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def toString: String = s"${this.getClass.getCanonicalName}[${SpliceDataSourceV1.NAME}]"
}
