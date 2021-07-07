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

import java.util.Properties
import javax.jms.ConnectionFactory

import com.rabbitmq.jms.admin.RMQConnectionFactory
import io.confluent.kafka.jms.KafkaConnectionFactory
import org.apache.activemq.ActiveMQConnectionFactory

/**
  * Created by exa00015 on 24/12/18.
  */
trait ConnectionFactoryProvider {

  def createConnection(options:Map[String,String]):ConnectionFactory

}

class AMQConnectionFactoryProvider extends ConnectionFactoryProvider {

  override def createConnection(options: Map[String, String]): ConnectionFactory = {
    new ActiveMQConnectionFactory
  }
}

class IBMMQConnectionFactoryProvider extends ConnectionFactoryProvider {

  override def createConnection(options: Map[String, String]): ConnectionFactory = {
    import com.ibm.msg.client.wmq.common.CommonConstants
    import com.ibm.msg.client.jms.JmsConstants
    val cf = com.ibm.msg.client.jms.JmsFactoryFactory.getInstance(JmsConstants.WMQ_PROVIDER).createConnectionFactory
    cf.setStringProperty(CommonConstants.WMQ_HOST_NAME, options.getOrElse("host", throw new IllegalArgumentException("Option host is required")))
    cf.setIntProperty(CommonConstants.WMQ_PORT, options.getOrElse("port", throw new IllegalArgumentException("Option port is required")).toInt)
    cf.setStringProperty(CommonConstants.WMQ_CHANNEL, options.getOrElse("channel", throw new IllegalArgumentException("Option channel is required")))
    cf.setIntProperty(CommonConstants.WMQ_CONNECTION_MODE, CommonConstants.WMQ_CM_CLIENT)
    cf.setStringProperty(CommonConstants.WMQ_QUEUE_MANAGER, options.getOrElse("queue_manager", throw new IllegalArgumentException("Option queue_manager is required")))
    options.get("application").foreach(
      app => cf.setStringProperty(CommonConstants.WMQ_APPLICATIONNAME, app)
    )
    cf.setBooleanProperty(JmsConstants.USER_AUTHENTICATION_MQCSP, true)
    options.get("userid").foreach(
      userid => cf.setStringProperty(JmsConstants.USERID, userid)
    )
    options.get("password").foreach(
      password => cf.setStringProperty(JmsConstants.PASSWORD, password)
    )
    cf
  }
}

class RMQConnectionFactoryProvider extends ConnectionFactoryProvider {

  override def createConnection(options: Map[String, String]): ConnectionFactory = {
    val connectionFactory = new RMQConnectionFactory
    connectionFactory.setUsername(options.getOrElse("username",throw new IllegalArgumentException("Option username is required")))
    connectionFactory.setPassword(options.getOrElse("password",throw new IllegalArgumentException("Option password is required")))
    connectionFactory.setVirtualHost(options.getOrElse("virtualhost",throw new IllegalArgumentException("Option virtualhost is required")))
    connectionFactory.setHost(options.getOrElse("host",throw new IllegalArgumentException("Option host is required")))
    connectionFactory.setPort(options.getOrElse("port",throw new IllegalArgumentException("Option port is required")).toInt)
    connectionFactory
  }
}

class KafkaConnectionFactoryProvider extends ConnectionFactoryProvider {

  override def createConnection(options: Map[String, String]): ConnectionFactory = {
    val props = new Properties
    props.put("bootstrap.servers", options.getOrElse("bootstrap.servers",throw new IllegalArgumentException("Option bootstrap.servers is required")))
    props.put("zookeeper.connect", options.getOrElse("zookeeper.connect",throw new IllegalArgumentException("Option zookeeper.connect is required")))
    props.put("client.id", options.getOrElse("client.id",throw new IllegalArgumentException("Option client.id is required" )))
    val connectionFactory = new KafkaConnectionFactory(props)
    connectionFactory
  }

}

