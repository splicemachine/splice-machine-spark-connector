//import java.io.Externalizable
import java.util
import java.util.{Collections, Properties, UUID}
import java.sql.{Connection, DriverManager, ResultSet}
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit.SECONDS

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.LongDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}

import scala.collection.JavaConverters._

object MessageCounter {
  // Kafka: Read kafka from beginning, keep min and max timestamp, maintain count
  // DB: Get min timestamp, get count and max ts

  // Init: in a loop, poll kafka and db, loop until both have records.  Keep start ts and count for each.

  // Poll each and count, calculate rate

  var kConsumer: KafkaConsumer[String, String] = _
//  var dbConn: Connection = _
  var dbkConsumer: KafkaConsumer[Integer, Long] = _
  
  var kTPartitions: mutable.Buffer[TopicPartition] = _

//  var spliceTable: String = _

  var kafkaTotal: Long = 0L
  var dbTotal: Long = 0L
  
  var kafkaRate: Double = 0.0
  var dbRate: Double = 0.0
  
  val windowSize = 5
  val kafkaRateWindow = mutable.Queue.fill(windowSize)(0.0)
  val dbRateWindow = mutable.Queue.fill(windowSize)(0.0)
  
  var kafkaStart: LocalDateTime = _
  var dbStart: LocalDateTime = _

  /**
    * 
    * @param args(0) kafkaServers
    * @param args(1) topicName
    * @param args(2) splice jdbcUrl
    * @param args(3) spliceTable
    */
  def main(args: Array[String]) {
    try {
      println( "Connecting to Kafka" )
      kConsumer = initKafka(args(0), args(1))
      
      println( "Connecting to Splice" )
//      dbConn = initDb(args(2))  //"jdbc:splice://localhost:1527/splicedb;user=splice;password=admin")
      dbkConsumer = initDb(args(0), "ssds-metrics")  //"jdbc:splice://localhost:1527/splicedb;user=splice;password=admin")

//      spliceTable = args(3)

//      println( "Init" )
//      init

      println( "Processing" )
      process
    } catch {
      case e: Exception => {
        val msg = "MessageCounter: Problem processing." +
          "\n" +
          e.toString
        println(msg)
        throw e
      }
      case t: Throwable => {
        val msg = "MessageCounter: Problem processing." +
          "\n" +
          t.toString
        println(msg)
        throw t
      }
    } finally {
      close
    }
  }
  
//  def init(): Unit = {
//    var kafkaReady = false
//    var dbReady = false
//    while( (!kafkaReady) || (!dbReady) ) {
//      kafkaTotal = kafkaCount
//      dbTotal = dbCount
//      if( (!kafkaReady) && kafkaTotal > 0 ) {
//        kafkaStart = now
//        kafkaReady = true
//      }
//      if( (!dbReady) && dbTotal > 0 ) {
//        dbStart = now
//        dbReady = true
//      }
//      Thread.sleep(1000)
//    }
//  }

  def now(): LocalDateTime = LocalDateTime.now
  var kt0: LocalDateTime = _
  var kt1: LocalDateTime = _
  var dt0: LocalDateTime = _
  var dt1: LocalDateTime = _
  var t0: LocalDateTime = _
  
  def process(): Unit = {
    t0 = now
    kt0 = now
    dt0 = now
    var count = 0L
    var prevKcount = 0L
    displayHeader
    while(true) {
//      println(s"\n${(new java.util.Date).toString} Check DB")
      count = dbCount
      dt1 = now
      dbTotal += count
      dbRate = count.asInstanceOf[Double] / SECONDS.between(dt0, dt1)
      dt0 = dt1

//      println(s"${(new java.util.Date).toString} Check Kafka")
      count = kafkaCount
      kt1 = now
      kafkaTotal = count
      kafkaRate = (count-prevKcount).asInstanceOf[Double] / SECONDS.between(kt0, kt1)
      kt0 = kt1
      prevKcount = count

      display
      Thread.sleep(1000)
    }
  }

  def displayHeader(): Unit = {
    println(" ")
    //println(s"${(new java.util.Date).toString}")
    println("Tm(s)\tIn Ttl\tIn Rt\tIn Rt Min5\tIn Rt Avg5\tIn Rt Max5\tDB Ttl\tDB Rt\tDB Rt Min5\tDB Rt Avg5\tDB Rt Max5\tLag")
    //    println("\t\tAvg In Rate /s\t\tAvg Table Rate /s")
  }
  
  def display(): Unit = {
//    println(" ")
//    println("****************")
//    println((new java.util.Date).toString)
//    println(" ")
//    println("Incoming Total")
//    println( kafkaTotal )
//    println("Incoming Rate")
//    println( kafkaTotal.asInstanceOf[Double] / SECONDS.between(kafkaStart, now) )
//    println(" ")
//    println("In Table Total")
//    println( dbTotal )
//    println("In Table Rate")
//    println( dbTotal.asInstanceOf[Double] / SECONDS.between(dbStart, now) )
//    println(" ")
//    println("Records in Process")
//    println( kafkaTotal - dbTotal )
    
//    val kafkaRate = kafkaTotal.asInstanceOf[Double] / SECONDS.between(kafkaStart, now)
//    val dbRate = dbTotal.asInstanceOf[Double] / SECONDS.between(dbStart, now)
    
    kafkaRateWindow.dequeue
    kafkaRateWindow.enqueue(kafkaRate)
    dbRateWindow.dequeue
    dbRateWindow.enqueue(dbRate)

    println(
      SECONDS.between(t0, now) +"\t"+
      kafkaTotal +"\t"+
      kafkaRate.round +"\t"+
      kafkaRateWindow.min.round +"\t"+
      (kafkaRateWindow.sum / windowSize).round +"\t"+
      kafkaRateWindow.max.round +"\t"+
        
      dbTotal +"\t"+
      dbRate.round +"\t"+
      dbRateWindow.min.round +"\t"+
      (dbRateWindow.sum / windowSize).round +"\t"+
      dbRateWindow.max.round +"\t"+
      (kafkaTotal - dbTotal) //+" ~ "+ (kafkaTotal - dbTotal).asInstanceOf[Double]/kafkaTotal
    )
//    println(
//      kafkaRateWindow.min.round +" - "+
//      (kafkaRateWindow.sum / windowSize).round +" - "+
//      kafkaRateWindow.max.round +
//      "\t" +
//      dbRateWindow.min.round +" - "+
//      (dbRateWindow.sum / windowSize).round +" - "+
//      dbRateWindow.max.round
//    )

//    println( kafkaTotal +"\t"+
//      kafkaRate.round +"\t\t"+
//      dbTotal +"\t"+
//      dbRate.round +"\t"+
//      (kafkaTotal - dbTotal) +" ~ "+ (kafkaTotal - dbTotal).asInstanceOf[Double]/kafkaTotal
//    )
//    println( //"\t\t" +
//      kafkaRateWindow.min.round +" - "+
//        (kafkaRateWindow.sum / windowSize).round +" - "+
//        kafkaRateWindow.max.round +
//        "\t" +
//        dbRateWindow.min.round +" - "+
//        (dbRateWindow.sum / windowSize).round +" - "+
//        dbRateWindow.max.round
//    )
  }
  
  def close(): Unit = {
    kConsumer.close
//    dbConn.close
    dbkConsumer.close
  }
  
  def initKafka(kafkaServers: String, topicName: String): KafkaConsumer[String, String] = {
    val props = new Properties()
    val groupId = "spark-consumer-ssds-msgcounter-inbound"
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, groupId + "-" + UUID.randomUUID())
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000000" )

    val kConsumer = new KafkaConsumer[String, String](props)
//    kConsumer.subscribe(util.Arrays.asList(topicName))  // for polling
    
    val partitionInfo = kConsumer.partitionsFor(topicName).asScala
    kTPartitions = partitionInfo.map(pi => new TopicPartition(topicName, pi.partition()))
    kConsumer.assign(kTPartitions.asJava)
    
    kConsumer
  }
  
  def initDb(kafkaServers: String, topicName: String): KafkaConsumer[Integer, Long] = {
//  def initDb(jdbcUrl: String): Connection = {
////    JdbcUtils.createConnectionFactory(new JDBCOptions(Map(
////      JDBCOptions.JDBC_TABLE_NAME -> "placeholder",
////      JDBCOptions.JDBC_URL -> "jdbc:splice://localhost:1527/splicedb;user=splice;password=admin"
////    )))()
//    Class.forName("com.splicemachine.db.jdbc.ClientDriver40")
//    DriverManager.getConnection( jdbcUrl )
    
    val props = new Properties()
    val groupId = "spark-consumer-ssds-msgcounter-dbins"
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, groupId + "-" + UUID.randomUUID())
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[IntegerDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[LongDeserializer].getName)

    val dbkConsumer = new KafkaConsumer[Integer, Long](props)
    dbkConsumer.subscribe(util.Arrays.asList(topicName))
    dbkConsumer
  }
  
  def kafkaCount(): Long = { //kConsumer.poll(1000).count
    kConsumer.seekToEnd(Collections.emptySet())
    val endPartitions: Map[TopicPartition, Long] = kTPartitions.map(p => p -> kConsumer.position(p))(collection.breakOut)
    kConsumer.seekToBeginning(Collections.emptySet())
    kTPartitions.map(p => endPartitions(p) - kConsumer.position(p)).sum
//    kTPartitions.map(p => kConsumer.position(p)).sum
  }

//  def dbCount(): Long = {
//    var rs: ResultSet = null
//    try {
//      rs = dbConn.createStatement.executeQuery(s"select count(tm) from $spliceTable")
//      rs.next
//      rs.getInt(1)
//    }
//    finally {
//      if( rs != null ) rs.close
//    }
//  }
//
  
  private val dbCountPollTimeout = java.time.Duration.ofMillis(java.lang.Long.MAX_VALUE)

  def dbCount(): Long = dbkConsumer.poll(dbCountPollTimeout).asScala.map(_.value).sum
//  {
//    try {
//      val records = dbkConsumer.poll(dbCountPollTimeout).asScala // records: Iterable[ConsumerRecord[Integer, Externalizable]]
//      records.map(_.value).sum
//    }
//    finally {
////      if( rs != null ) rs.close
//    }
//  }

//    {
////    var records = Iterable.empty[ConsumerRecord[Integer, Externalizable]]
//    var newRecords = consumer.poll(timeout).asScala // records: Iterable[ConsumerRecord[Integer, Externalizable]]
////    records = records ++ newRecords
//
////    while (newRecords.nonEmpty) {
////      newRecords = consumer.poll(shortTimeout).asScala // records: Iterable[ConsumerRecord[Integer, Externalizable]]
////      records = records ++ newRecords
////    }
//    consumer.close
//
//    newRecords.size
//  }

//  def dbConnection(table: String): Connection =
//    JdbcUtils.createConnectionFactory(new JDBCOptions(Map(
//      JDBCOptions.JDBC_TABLE_NAME -> "placeholder",
//      JDBCOptions.JDBC_URL -> "jdbc:splice://localhost:1527/splicedb;user=splice;password=admin"
//    )))()
//    val internalOptions = Map(
//      JDBCOptions.JDBC_TABLE_NAME -> "placeholder",
//      JDBCOptions.JDBC_URL -> "jdbc:splice://localhost:1527/splicedb;user=splice;password=admin"
//    )
//    val internalJDBCOptions = new JDBCOptions(internalOptions)
//    val conn = JdbcUtils.createConnectionFactory(internalJDBCOptions)()
  
  //    var rs: ResultSet = null
//    try {
//      rs = conn.createStatement().executeQuery(s"select count(*) from $table")
//      rs.next
//      rs.getInt(1)
//    }
//    finally {
//      rs.close
//      conn.close
//    }
//  }

}
