import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

object SSDSExcLogScanner {
  def main(args: Array[String]) {
    if( args.length < 1 ) {
      throw new Exception( "Missing first arg (required): path to log file" )
    }
    
    val logFile = args(0)
    val startDt = args(1)
    
    val lines = Files.lines( Paths.get(logFile) )
    val lineItrs = lines.iterator().asScala
    
    process(lineItrs, startDt)

    lines.close
  }
  
  private def process(lineItr: Iterator[String], startDt: String): Unit = {
    val splits = lineItr
//      .filter( ln => ln.compareTo(startDt) > 0 )
      .filter( ln => ln.contains("SMC.sendData t ") )
      .map( ln => ln.split(" ") )

    val emptyMap = Map[String,Int]().withDefaultValue(0)

    val topicPartitionCount = splits
      .map( lnry => {
        val last = lnry.length - 1
        (s"${lnry(last-4)}:${lnry(last-2)}", lnry(last).toInt )  // (topic:partition, count)
      } )
      .foldLeft(emptyMap)((map,item) => {
        map.updated(item._1, item._2 + map(item._1))
      })
      .toSeq
      .sorted
      .mkString("\n")

    println(s"$topicPartitionCount")
  }
}
