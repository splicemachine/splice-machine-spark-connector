package org.apache.spark.sql.jms

import javax.jms._
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import com.splicemachine.spark.jms.{DefaultSource, JmsMessage, JmsSourceOffset}

import scala.collection.mutable.ListBuffer

/**
  * Created by exa00015 on 25/12/18.
  */

class JmsStreamingSource(sqlContext: SQLContext,
                         parameters: Map[String, String],
                         metadataPath: String,
                         failOnDataLoss: Boolean
                        ) extends Source {

  var counter = sqlContext.sparkContext.longAccumulator("counter")

  lazy val RECEIVER_TIMEOUT = parameters.getOrElse("receiver.timeout", "3000").toLong


  val connection = DefaultSource.connectionFactory(parameters).createConnection
  val session: Session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE)
  connection.start

  override def schema: StructType = {
    ScalaReflection.schemaFor[JmsMessage].dataType.asInstanceOf[StructType]
  }

  override def getOffset: Option[Offset] = {
    counter.add(1)
    Some(JmsSourceOffset(counter.value))
  }
  
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val queue = session.createQueue(parameters("queue"))
    val consumer = session.createConsumer(queue)
    var retrieving = true
    val messageList: ListBuffer[JmsMessage] = ListBuffer()
    val ack = parameters.getOrElse("acknowledge", "false").toBoolean
    def getMessage: Option[JmsMessage] = consumer.receive(RECEIVER_TIMEOUT) match {
      case null => None
      case msg: Message => {
        if(ack) { msg.acknowledge }
        Some(
          parameters("message_type") match {
            case "bytes" => JmsMessage( msg.asInstanceOf[BytesMessage] )
            case _ => JmsMessage( msg.asInstanceOf[TextMessage] )
          }
        )
      }
    }
    while (retrieving) {
      getMessage match {
        case Some(msg) => messageList += msg
        case None => retrieving = false
      }
    }
    import org.apache.spark.unsafe.types.UTF8String._
    val internalRDD = messageList.map(message => InternalRow(
      fromString(message.content),
      fromString(message.correlationId),
      fromString(message.jmsType),
      fromString(message.messageId),
      fromString(message.queue)
    ))
    val rdd = sqlContext.sparkContext.parallelize(internalRDD)
    sqlContext.internalCreateDataFrame(rdd, schema, true)
  }

  override def stop(): Unit = {
    session.close
    connection.close
  }

}

