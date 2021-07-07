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

import javax.jms.Session
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink

/**
  * Created by exa00015 on 26/12/18.
  */
class JmsStreamingSink(sqlContext: SQLContext,
                       parameters: Map[String, String]
                      ) extends Sink {

  @volatile private var latestBatchId = -1L


  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= latestBatchId) {

    } else {
      data.foreachPartition((rowIter: Iterator[Row]) => {
        val connection = DefaultSource.connectionFactory(parameters).createConnection
        val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
        val queue = session.createQueue(parameters.getOrElse("queue",throw new IllegalArgumentException("Option queue is required")))
        val producer = session.createProducer(queue)
        rowIter.foreach(
          record => {
            val msg = session.createTextMessage(record.toString())
            producer.send(msg)
          })
        producer.close
        connection.close
        session.close
      }
      )
      latestBatchId = batchId
    }
  }

}
