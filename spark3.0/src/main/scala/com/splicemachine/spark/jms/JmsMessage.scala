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
