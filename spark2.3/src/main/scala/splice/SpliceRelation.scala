package splice

import _root_.com.splicemachine.spark2.splicemachine.SplicemachineContext
//import com.splicemachine.db.impl.sql.execute.ValueRow
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
    val spliceCtx = new SplicemachineContext(opts.url)  // TODO splice
    // FIXME columnProjection is required
    println(s">>> relation scan rows: ${opts.table} $requiredColumns")
//    val rows = spliceCtx.rows(opts.table, requiredColumns)
//    def vr2r = (vr: ValueRow) => Row.fromSeq(vr.toSeq)
//    val rows = valueRows.map(vr => Row.fromSeq(Seq(vr.getString(0), vr.getString(1))))
//    val res = sparkSession.sparkContext.parallelize(rows)
//    val res = spliceCtx.rdd(opts.table, requiredColumns)
//    println(s">>> relation scan res: $res")
//    println(s">>> relation scan res: ${res.map(_.getClass.getName).first}")
//    println(s">>> relation scan res: ${res.take(5).foreach(println)}")
//    println(s">>> relation scan return")
//    res //.asInstanceOf[RDD[Row]]
    spliceCtx.rdd(opts.table, requiredColumns)
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    if( data.count > 0 ) {
      println(s">>> relation insert: ${opts.url}")
      val spliceCtx = new SplicemachineContext(opts.url) // TODO splice
      val tableName = opts.table
      val isTableAvailable = spliceCtx.tableExists(tableName)
      println(s">>> relation insert table available: $isTableAvailable")
      if (!isTableAvailable) {
        spliceCtx.createTable(tableName, data.schema, keys = Seq.empty, createTableOptions = "UNUSED")
      } else if (isTableAvailable && overwrite) {
        println(s">>> relation insert drop and create")
        spliceCtx.dropTable(tableName)
        spliceCtx.createTable(tableName, data.schema, keys = Seq.empty, createTableOptions = "UNUSED")
      }
//      println(s">>> relation insert: $tableName $data")
//      println(s">>> relation insert: ${data.show}")
//      data.show
      
      spliceCtx.insert(data, tableName)
//      spliceCtx.bulkImportHFile(data, tableName, scala.collection.mutable.Map("bulkImportDirectory" -> "/tmp/bulkImport"))
      println(s">>> relation inserted")
    }
  }

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def toString: String = s"${this.getClass.getCanonicalName}[${SpliceDataSource.NAME}]"
}
