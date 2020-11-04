import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

object SSDSLogScanner {
  def main(args: Array[String]) {
    if( args.length < 1 ) {
      throw new Exception( "Missing first arg (required): path to log file" )
    }
    
    val logFile = args(0)
    val startDt = args(1)
    
    val lines = Files.lines( Paths.get(logFile) )
    val itr12 = lines.iterator().asScala.duplicate
    val itr34 = itr12._2.duplicate

    process(itr12._1, startDt, " transferred batch having ", "SSDS" )
    process(itr34._1, startDt, " INS task ", "NSDS" )  //, s => s.substring(0, s.length-1) )
    process(itr34._2, startDt, " SMC.inss topicCount postex ", "Splice Kafka" )

    lines.close
  }
  
  private def process(lineItr: Iterator[String], startDt: String, key: String, label: String, f: String => String = identity): Unit = {
    val splits = lineItr
      .filter( ln => ln.compareTo(startDt) > 0 )
      .filter( ln => ln.contains(key) )
      .map( ln => ln.split(" ") )

    val recordCount = splits
      .map( lnry => lnry(lnry.length - 1) )
      .map( f )
      .map( _.toLong )
      .sum

    println(s"$recordCount\t$label")
  }
}
