package com.spicemachine.spark.ingester

import java.util.concurrent.LinkedBlockingQueue

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

import org.apache.spark.sql.DataFrame
import com.splicemachine.spark2.splicemachine.SplicemachineContext

class ParallelInsert {

  val inserters = new LinkedBlockingQueue[SplicemachineContext]()
  
  def add(smc: SplicemachineContext): Unit = inserters.put(smc)

  def insert(df: DataFrame, spliceTable: String): Unit = {
    spliceInsert(inserters.take, df, spliceTable).onComplete {
      case Success(smc) => inserters.put(smc)
      case Failure(e) => e.printStackTrace
    }
  }

  private def spliceInsert(smc: SplicemachineContext, df: DataFrame, spliceTable: String): Future[SplicemachineContext] = Future {
    smc.insert(df, spliceTable)
    smc
  }
}
