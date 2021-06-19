package com.spicemachine.spark.ingester.component

import org.apache.spark.sql.DataFrame

trait LoadAnalyzer {
  def newTopic(topic: String)
  def analyze(df: DataFrame)
}
