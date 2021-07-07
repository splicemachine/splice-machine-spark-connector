/*
 * Copyright 2020 https://github.com/jksinghpro/spark-jms
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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * Created by exa00015 on 24/12/18.
  */
class DataFrameRelation(override val sqlContext: SQLContext,data:DataFrame) extends BaseRelation with TableScan with Serializable {

  override def schema: StructType = {
    data.schema
  }

  override def buildScan(): RDD[Row] = {
    data.rdd
  }

}
