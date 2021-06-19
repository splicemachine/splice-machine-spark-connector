package com.spicemachine.spark.ingester.component

trait InsertCompleteAction {
  def insertComplete(topicInfo: String)
}
