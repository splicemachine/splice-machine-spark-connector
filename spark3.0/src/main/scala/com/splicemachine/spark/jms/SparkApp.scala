/*
 * Copyright 2020 https://github.com/jksinghpro/spark-jms
 * Copyright 2021 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.splicemachine.spark.jms

import java.io.StringReader

import javax.xml.namespace.QName
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamConstants
import javax.xml.stream.events.XMLEvent
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
//import org.apache.spark.sql.types._

/**
  * Created by exa00015 on 21/12/18.
  */
object SparkApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Sql starter")
    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)
    val sparkSession = sqlContext.sparkSession
    import sparkSession.implicits._

    val stream = sparkSession.readStream
      .format("jk.bigdata.tech.jms")
      .option("connection","ibmmq")
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
        println("**********")
        println(xml)
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
      }) (RowEncoder(
          StructType(Seq(
            StructField("store_id", StringType),
            StructField("transaction_date", StringType),
            StructField("workstation_id", StringType),
            StructField("transaction_number", StringType),
            StructField("transaction_type", StringType),
            StructField("klog_xml", StringType),
            StructField("created_ts", StringType),
            StructField("created_by", StringType)
          ))
      ))

//    BusinessUnit.UnitID — store_id string
//    ReceiptDateTime — transaction_date timestamp
//    WorkstationID — workstation_id numeric(8,0)
//    SequenceNumber — transaction_number numeric(8,0)
//    Transaction.TypeCode — transaction_type string
//    Entire xml — klog_xml clob
//    POSLogDateTime — created_ts timestamp
//    OperatorID — created_by string

    val query = stream.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination
    query.stop
    sparkSession.stop

    /*
    import sparkSession.implicits._
    val df = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "connect-configs")
      .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val query = df.writeStream
      .outputMode("append")
      .format("console")
      .start()
    //result.show(20,false)
    /*result.write.mode(SaveMode.Append)
      .format("jk.bigdata.tech.jms")
      .option("connection","jk.bigdata.tech.jms.AMQConnectionFactoryProvider")
      .option("queue","customerQueue")
      .save*/
      */
  }

}
