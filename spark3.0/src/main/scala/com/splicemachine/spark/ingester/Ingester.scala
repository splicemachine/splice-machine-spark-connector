package com.spicemachine.spark.ingester

import org.apache.spark.sql.DataFrame

trait Ingester {
  def ingest(df: DataFrame)
  def stop()
}
