package com.spicemachine.spark.ingester.component

import java.util.concurrent.LinkedTransferQueue

class InsertedTimestampTracker(
                                queue: LinkedTransferQueue[String]
                              ) extends InsertCompleteAction
{
  def insertComplete(topicInfo: String): Unit = queue.put(topicInfo)
}
