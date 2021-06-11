package com.splicemachine.spark.driver

import java.io.StringReader

import javax.xml.namespace.QName
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamConstants
import javax.xml.stream.events.XMLEvent
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
//import org.apache.spark.sql.types._
import com.spicemachine.spark.ingester.SLIIngester

/**
  */
object JMSReaderApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("JMSReaderApp")
    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)
    val sparkSession = sqlContext.sparkSession
    import sparkSession.implicits._
    
    val schema = StructType(Seq(
      StructField("store_id", StringType),
      StructField("transaction_date", StringType),
      StructField("workstation_id", StringType),
      StructField("transaction_number", StringType),
      StructField("transaction_type", StringType),
      StructField("klog_xml", StringType),
      StructField("created_ts", StringType),
      StructField("created_by", StringType)
    ))

    val stream = sparkSession.readStream
      .format("com.splicemachine.spark.jms")
      .option("connection","ibmmq")     // TODO pass as params to main
      .option("host","stl-colo-srv006")
      .option("port","1414")
      .option("channel","DEV.APP.SVRCONN")
      .option("queue_manager","QM1")
      .option("queue","DEV.QUEUE.1")
      .option("message_type","bytes")
      .option("userid","app")
      .option("password","passw0rd")
      .option("application","SparkApp0")
      .load
      //      .select(col("content") as "klog_xml")
      .map( row => {
        val xml = row.getAs[String]("content")
        val eventReader = XMLInputFactory.newInstance.createXMLEventReader(new StringReader(xml))
        var inKlog = false
        var inTransaction = false
        var inBusinessUnit = false
        val data = collection.mutable.Map[String,String]()
        data += "klog_xml" -> xml
        while( data.size < 8 && eventReader.hasNext ) {
          val event = eventReader.nextEvent
          event.getEventType match {
            case XMLStreamConstants.START_ELEMENT => {
              val startElement = event.asStartElement
              startElement.getName.getLocalPart.toUpperCase match {
                case "KLOG" => inKlog = true
                case "TRANSACTION" => if( inKlog ) {
                  inTransaction = true
                  data += ("transaction_type" -> startElement.getAttributeByName(new QName("TypeCode")).getValue)
                }
                case "BUSINESSUNIT" => if( inTransaction ) { inBusinessUnit = true }
                case "UNITID" => if( inBusinessUnit ) {
                  data += ("store_id" -> eventReader.getElementText)
                }
                case "RECEIPTDATETIME" => if( inTransaction ) {
                  data += ("transaction_date" -> eventReader.getElementText)
                }
                case "WORKSTATIONID" => if( inTransaction ) {
                  data += ("workstation_id" -> eventReader.getElementText)
                }
                case "SEQUENCENUMBER" => if( inTransaction ) {
                  data += ("transaction_number" -> eventReader.getElementText)
                }
                case "POSLOGDATETIME" => if( inTransaction ) {
                  data += ("created_ts" -> eventReader.getElementText)
                }
                case "OPERATORID" => if( inTransaction ) {
                  data += ("created_by" -> eventReader.getElementText)
                }
                case _ => {}
              }
            }
            case XMLStreamConstants.END_ELEMENT => {
              val endElement = event.asEndElement
              endElement.getName.getLocalPart.toUpperCase match {
                case "KLOG" => inKlog = false
                case "TRANSACTION" => if (inKlog) { inTransaction = false }
                case "BUSINESSUNIT" => if (inTransaction) { inBusinessUnit = false }
                case _ => {}
              }
            }
            case _ => {}
          }
        }
        Row(
          data.getOrElse("store_id", ""),
          data.getOrElse("transaction_date", ""),
          data.getOrElse("workstation_id", ""),
          data.getOrElse("transaction_number", ""),
          data.getOrElse("transaction_type", ""),
          data.getOrElse("klog_xml", ""),
          data.getOrElse("created_ts", ""),
          data.getOrElse("created_by", "")
        )
      }) (RowEncoder(schema))

    //    BusinessUnit.UnitID — store_id string
    //    ReceiptDateTime — transaction_date timestamp
    //    WorkstationID — workstation_id numeric(8,0)
    //    SequenceNumber — transaction_number numeric(8,0)
    //    Transaction.TypeCode — transaction_type string
    //    Entire xml — klog_xml clob
    //    POSLogDateTime — created_ts timestamp
    //    OperatorID — created_by string

//    val query = stream.writeStream
//      .outputMode("append")
//      .format("console")
//      .start()

    val transactionSink = new SLIIngester(1, 2,
      schema,
      "jdbc:splice://localhost:1527/splicedb;user=splice;password=admin",
      "transaction_table",
      "localhost:9092",
      1,  // equal to number of partition in DataFrame?
      false, // upsert: Boolean
      true  // loggingOn: Boolean
    )

    val exceptionSink = new SLIIngester(1, 2,
      schema,
      "jdbc:splice://localhost:1527/splicedb;user=splice;password=admin",
      "exception_table",
      "localhost:9092",
      1,  // equal to number of partition in DataFrame?
      false, // upsert: Boolean
      true  // loggingOn: Boolean
    )
    
    def isExceptionRecord: Row => Boolean = row => row.getAs[String]("transaction_type").startsWith("20_")
    def isTransactionRecord: Row => Boolean = row => !isExceptionRecord(row)

    val strQuery = stream
      .writeStream
//      .option("checkpointLocation",s"/tmp/checkpointLocation-$spliceTable-${java.util.UUID.randomUUID()}")
      .option("checkpointLocation",s"/tmp/checkpointLocation-${java.util.UUID.randomUUID()}")
      .foreachBatch {
        (batchDF: DataFrame, batchId: Long) => try {
          transactionSink.ingest( batchDF.filter( isTransactionRecord ) )
          exceptionSink.ingest( batchDF.filter( isExceptionRecord ) )
        } catch {
          case e: Throwable =>
            e.printStackTrace
        }
      }.start()

    strQuery.awaitTermination
    strQuery.stop
    sparkSession.stop
  }
}
