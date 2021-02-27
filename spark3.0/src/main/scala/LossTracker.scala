import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

object LossTracker {
  def main(args: Array[String]) {
    if (args.length < 3) {
      throw new Exception("Missing args (required): path to log file")
    }

    val producerLog = args(0)
    val kafkaLog = args(1)
    val consumerLog = args(2)

    val pLines = Files.lines(Paths.get(producerLog))
    val kLines = Files.lines(Paths.get(kafkaLog))
    val cLines = Files.lines(Paths.get(consumerLog))

    process(
      kLines.iterator.asScala,
      mapOf(pLines.iterator.asScala),
      mapOf(cLines.iterator.asScala)
    )

    pLines.close
    kLines.close
    cLines.close
  }

  private def mapOf(lineItr: Iterator[String]): Map[String, Int] = {
    val emptyMap = Map[String, Int]().withDefaultValue(0)
    lineItr
      .filter(ln => ln.startsWith("("))
      .map(ln => {
        val s = ln.split("[(,)]")
        //println(s"${s.length} $ln")
        (s(1), s(2).toInt)  // (topic:partition, count)
      })
      .foldLeft(emptyMap)((map, item) => {
        map.updated(item._1, item._2 + map(item._1))
      })
  }

  private def process(kLineItr: Iterator[String], pMap: Map[String, Int], cMap: Map[String, Int]): Unit =
    kLineItr.foreach(ln => {
      val s = ln.split(":")
      val topic = s(0)
      val partition = s(1)
      val kafkaCount = s(2).toInt
      val tp = topic + ":" + partition
      val pdrCount = pMap.get(tp).getOrElse(0)
      val csrCount = cMap.get(tp).getOrElse(0)
      val out = s"$topic,$partition,$pdrCount,$kafkaCount,${pdrCount - kafkaCount},$csrCount,${kafkaCount - csrCount}"
      println(out)
    })

}
