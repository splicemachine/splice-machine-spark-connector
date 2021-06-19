package com.spicemachine.spark.ingester.component

import java.sql.Timestamp
import java.util.concurrent.LinkedTransferQueue

import org.apache.spark.sql.DataFrame

class LoadedTimestampTracker(
                                queue: LinkedTransferQueue[(Timestamp,String)],
                                windowMs: Long
                              ) extends LoadAnalyzer
{
  private var analyzed = false
  private var topic = ""
  private var lastPublishedFinishWn = new Timestamp(1)
  private var earliestWnInTopic: Option[Timestamp] = None  //Timestamp.from(java.time.Instant.now.plusSeconds(60*60*24*365*10))
  
  def newTopic(newTopicName: String): Unit = {
    if(!topic.isEmpty) {
      //println(s"NewTopic not empty $lastPublishedFinishWn $earliestWnInTopic")
      if(earliestWnInTopic.isDefined && earliestWnInTopic.get.after(lastPublishedFinishWn)) {
        queue.put((earliestWnInTopic.get, topic))
        lastPublishedFinishWn = earliestWnInTopic.get
      }
      earliestWnInTopic = None
    } else {
//      if(analyzed) {
//        lastPublishedFinishWn = earliestWnInTopic
//      }
      //println(s"NewTopic empty $lastPublishedFinishWn $earliestWnInTopic")
    }
    topic = newTopicName
  }

  def windowOf(ms: Long): (Timestamp,Timestamp) = {
    val wndStart = ms - (ms % windowMs)
    (new Timestamp(wndStart), new Timestamp(wndStart + windowMs))
  }

  def analyze(df: DataFrame): Unit = {
    try {
      //println(s"Analyze Start")
      if (!df.isEmpty) {
        //println(s"Analyze df not empty")
        val minStart = df
          .reduce((r1, r2) => {
            val t1 = r1.getAs[Timestamp]("START_TS").getTime
            val t2 = r2.getAs[Timestamp]("START_TS").getTime
            if (t1 < t2) {
              r1
            } else {
              r2
            }
          })
        //println(s"Analyze minStart $minStart")
        val finishWn = windowOf(minStart.getAs[Timestamp]("START_TS").getTime - 1)._1
        //println(s"Analyze finishWn $finishWn")
        //println(s"Analyze earliestWnInTopic $earliestWnInTopic")
        if( earliestWnInTopic.isEmpty ) {
          earliestWnInTopic = Some(finishWn)
        }
        if( finishWn.before(earliestWnInTopic.get) ) {
          earliestWnInTopic = Some(finishWn)
        }
        if(!analyzed) {
          lastPublishedFinishWn = earliestWnInTopic.get
        }
        analyzed = true
      }
      //println(s"Analyze End")
    } catch {
      case e: Throwable => println(s"Analyze exception $e")
    }
  }
}
