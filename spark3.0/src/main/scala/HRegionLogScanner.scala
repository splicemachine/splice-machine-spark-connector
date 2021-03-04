//import org.apache.spark.sql.SparkSession
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

object HRegionLogScanner {
  def main(args: Array[String]) {
    if( args.length < 1 ) {
      throw new Exception( "Missing first arg (required): path to log file" )
    }
    
    val logFile = args(0)
    
//    val spark = SparkSession.builder.appName("Log Scanner").getOrCreate()
//    val logData = spark.read.textFile(logFile).cache()
//    
//    val numAs = logData.filter(line => line.contains("a")).count()
//    val numBs = logData.filter(line => line.contains("b")).count()
//    println(s"Lines with a: $numAs, Lines with b: $numBs")
//    println(s"args(0) ${args(0)}")
//    
//    spark.stop()

    
//    Stream should be closed 
//    val stream : Stream[Path] = Files.list(Paths.get(".")) 
//    val paths: List[Path] = stream.iterator().asScala.toList 
//    stream.close()

    val query = "insert into perf1"
    
    val lines = Files.lines( Paths.get(logFile) )

    val qry = lines.iterator().asScala
      .filter( ln => 
        (ln.contains("Start executing query") && ln.contains(query))
        || ln.contains("End executing query")
      ).toSeq
    
    def parseValue: (String,String) => String =
      (line, fieldName) => line.split(s"$fieldName=")(1).split(", ")(0)
      
    def uuid2ln: String => (String,String) = ln => parseValue(ln, "uuid") -> ln
        
    val startLogs = qry.filter( _.contains("Start executing query") )   
      .map( uuid2ln )
      .toMap

    val endLogs = qry.filter( _.contains("End executing query") )
      .map( uuid2ln )
      .toMap
    
//    startMap.foreach(println)
//    endMap.foreach(println)

    val region = logFile.split("[.]")(0)
    println(logFile)
    println(s"Stats on query: $query")
    println("log date, log time, timeSpent(ms), modifiedRows, rows / sec, badRecords")
    startLogs.keySet
        .map( uuid =>
          if( ! endLogs.contains(uuid) ) {
            s"End of query not found for $uuid"
          } else {
            val ln = endLogs(uuid)
            val w = ln.split(" ")
            val tmVal = parseValue(ln, "timeSpent")
            val tm = tmVal.substring(0, tmVal.length-"ms".length)
            val rows = parseValue(ln, "modifiedRows")
            val d = "\t"
            w(0) +d+ w(1) +d+ tm +d+ rows +d+ rows.toDouble / (tm.toDouble/1000) +d+ parseValue(ln, "badRecords") +d+ region
          } )
        .toSeq
        .sorted
        .foreach(println)
    
    lines.close
  }
}
