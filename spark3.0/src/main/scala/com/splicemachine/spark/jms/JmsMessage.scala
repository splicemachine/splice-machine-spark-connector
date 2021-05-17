package com.splicemachine.spark.jms

import com.ibm.msg.client.jms.JmsConstants
import javax.jms.{BytesMessage, MapMessage, ObjectMessage, StreamMessage, TextMessage}

import scala.beans.BeanProperty

/**
  * Created by exa00015 on 26/12/18.
  */
case class JmsMessage( @BeanProperty content:String, @BeanProperty correlationId:String, @BeanProperty jmsType:String,@BeanProperty messageId:String , @BeanProperty queue:String)


object JmsMessage {

  def apply(message:TextMessage): JmsMessage =
    new JmsMessage(message.getText, message.getJMSCorrelationID, message.getJMSType, message.getJMSMessageID,message.getJMSDestination.toString)

  def apply(message:BytesMessage): JmsMessage = {
    val TEXT_LENGTH = message.getBodyLength.intValue
    val textBytes = new Array[Byte](TEXT_LENGTH)
    message.readBytes(textBytes, TEXT_LENGTH)
    val codePage = message.getStringProperty(JmsConstants.JMS_IBM_CHARACTER_SET)
    new JmsMessage(new String(textBytes, codePage), message.getJMSCorrelationID, message.getJMSType, message.getJMSMessageID, message.getJMSDestination.toString)
  }
  
  def apply(message:MapMessage): JmsMessage = null

  def apply(message:ObjectMessage): JmsMessage = null

  def apply(message:StreamMessage): JmsMessage = null

  def apply(): JmsMessage = null

}
