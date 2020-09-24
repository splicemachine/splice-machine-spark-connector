import java.util
import java.util.concurrent.BlockingQueue

import scala.collection.mutable
import scala.collection.JavaConverters._

class BatchRegulation(
   batchCountQueue: BlockingQueue[Long]
 ) {

  val recentCtLimit = 5
  val recentCounts = mutable.Queue.empty[Long]
  var sizeLimit = Long.MaxValue

  def pass(size: Long): Boolean = {
    val list = new util.ArrayList[Long]()
    batchCountQueue.drainTo(list)
    list.asScala.filter( _ < sizeLimit ).foreach( recentCounts.enqueue(_) )
    Range(0, recentCounts.size - recentCtLimit).foreach( i => recentCounts.dequeue() )
    
    if( recentCounts.size < recentCtLimit ) {
      log("few counts")
      true
    } else {
      val msd = mean_stdDev(recentCounts)
      if( size.toDouble >= msd._1 + msd._2 ) {
        sizeLimit = (msd._1 + msd._2).floor.toLong
        log(s"large batch: ${size.toDouble} > limit $sizeLimit")
        false
      } else {
        log("in range")
        true
      }
    }
  }
  
  private def log(s: String): Unit =
    println(s"${java.time.Instant.now} BREG $s")

  def mean_stdDev(s: Iterable[Long]): (Double,Double) = {
    val mean = s.sum.toDouble / s.size
    (mean, math.sqrt(s.map(_.toDouble).map(a => math.pow(a - mean, 2)).sum / s.size) )
  }

//  def mean(s: Iterable[Long]): Double = s.sum.toDouble / s.size
//
//  def variance(s: Iterable[Long]): Double = {
//    val avg = mean(s)
//    s.map(_.toDouble).map(a => math.pow(a - avg, 2)).sum / s.size
//  }
//
//  def stdDev(s: Iterable[Long]): Double = math.sqrt(variance(s))
}
