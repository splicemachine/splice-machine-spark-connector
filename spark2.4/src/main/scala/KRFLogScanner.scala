import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

object KRFLogScanner {
  def main(args: Array[String]) {
    if( args.length < 1 ) {
      throw new Exception( "Missing first arg (required): path to log file" )
    }
    
    val logFile = args(0)
    val startDt = args(1)
    
    val lines = Files.lines( Paths.get(logFile) )
    val lineItrs = lines.iterator().asScala.duplicate
    
    // <...> KRF.call p 4 t 470EF8CA332F3DDFD5FB00EC0DE88D9D71FA92DD6041CF82C4C35066B3ED45DF-19924525430016 expected 11999
    //val expected = process(lineItrs._1, startDt, " expected ")
    
    // <...> KRF.call p 19 t 470EF8CA332F3DDFD5FB00EC0DE88D9D71FA92DD6041CF82C4C35066B3ED45DF-19924525430016 records 100
    val actual = process(lineItrs._2, startDt, " records ")
    
//    println(s"\n$expected\tIn Kafka")
//    println(s"$actual\tRetrieved")

//    val splits = lines.iterator().asScala
//      .filter( ln => ln.compareTo(startDt) > 0 )
//      .filter( ln => 
//        ln.contains("KRF.call p") && ln.contains(" records ")
//      ).map( ln => ln.split(" ") )
//      .duplicate
//      
//    val recordCount = splits._1
//      .map( lnry => lnry(lnry.length - 1).toLong )
//      .sum
//    
//    println(recordCount)
//    
//    val partitions = splits._2
//      .map( lnry => lnry(lnry.length - 3) )
//      .toSeq
//      .distinct
//      .sorted
//      .mkString
//    
//    println(partitions)
    
    lines.close
  }
  
  private def process(lineItr: Iterator[String], startDt: String, key: String): Long = {
    val splits = lineItr
      .filter( ln => ln.compareTo(startDt) > 0 )
      .filter( ln =>
        ln.contains("KRF.call p") && ln.contains(key)
      ).map( ln => ln.split(" ") )
      .duplicate

    val emptyMap = Map[String,Int]().withDefaultValue(0)

    val topicPartitionCount = splits._1
      .map( lnry => {
        val last = lnry.length - 1
        (s"${lnry(last-2)}:${lnry(last-4)}", lnry(last).toInt )  // (topic:partition, count)
      } )
      .foldLeft(emptyMap)((map,item) => {
        map.updated(item._1, item._2 + map(item._1))
      })
      .toSeq
      .sorted
      .mkString("\n")
    
    println(s"$topicPartitionCount")

    val totalRecordCount = splits._2
      .map( lnry => lnry(lnry.length - 1).toLong )
      .sum

    println(s"$totalRecordCount\t$key")
    
    totalRecordCount
  }
}
