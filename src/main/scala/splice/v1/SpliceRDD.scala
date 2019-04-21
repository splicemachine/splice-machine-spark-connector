package splice.v1

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources.Filter

class SpliceRDD(
    sc: SparkContext,
    requiredColumns: Array[String],
    filters: Array[Filter])
  extends RDD[Row](sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    // FIXME Real values from the table to read from
    Iterator(Row.apply(0L, "splice machine"))
  }

  override protected def getPartitions: Array[Partition] = {
    // FIXME Real values for the table to read from
    Array(SplicePartition(0))
  }
}

case class SplicePartition(override val index: Int) extends Partition
