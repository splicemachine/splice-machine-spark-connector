package com.splicemachine.spark.driver

import org.apache.log4j.Logger
import scopt.OParser

case class Config (
                   appName: String = "",
                   externalKafkaServers: String = "",
                   externalTopic: String = "",
                   schemaDDL: String = "",
                   spliceUrl: String = "",
                   spliceTable: String = "",
                   spliceKafkaServers: String = "localhost:9092",
                   spliceKafkaPartitions: Int = 1,
                   numLoaders: Int = 1,
                   numInserters: Int = 1,
                   startingOffsets: String = "latest",
                   checkpointLocationRootDir: String = "/tmp",
                   upsert: Boolean = false,
                   eventFormat: String = "flat",
                   dataTransformation: Boolean = false,
                   tagFilename: String = "",
                   useFlowMarkers: Boolean = false,
                   maxPollRecs: String = "", //TODO ??
                   groupId: String = "",
                   clientId: String = "",
                   kwargs: Map[String, String] = Map())

class AppConfig(var args: Array[String]) {

  val log = Logger.getLogger(getClass.getName)

  val builder = OParser.builder[Config]

  val parser1 = {
    import builder._
    OParser.sequence(
      programName("scopt"),
      head("scopt", "4.x"),
      opt[String]('a', "appName")
        .action((x, c) => c.copy(appName = x))
        .text("application name"),
      opt[String]("externalKafkaServers")
        .action((x, c) => c.copy(externalKafkaServers = x)),
      opt[String]('t', "externalTopic")
        .action((x, c) => c.copy(externalTopic = x)),
      opt[String]('s', "schemaDDL")
        .action((x, c) => c.copy(schemaDDL = x)),
      opt[String]('u', "spliceUrl")
        .action((x, c) => c.copy(spliceUrl = x)),
      opt[String]("spliceTable")
        .action((x, c) => c.copy(spliceTable = x)),
      opt[String]("spliceKafkaServers")
        .action((x, c) => c.copy(spliceKafkaServers = x)),
      opt[Int]("spliceKafkaPartitions")
        .action((x, c) => c.copy(spliceKafkaPartitions = x)),
      opt[Int]("numLoaders")
        .action((x, c) => c.copy(numLoaders = x)),
      opt[Int]("numInserters")
        .action((x, c) => c.copy(numInserters = x)),
      opt[String]("startingOffsets")
        .action((x, c) => c.copy(startingOffsets = x)),
      opt[String]("checkpointLocationRootDir")
        .action((x, c) => c.copy(checkpointLocationRootDir = x)),
      opt[Unit]("upsert")
        .action((_, c) => c.copy(upsert = true))
        .text("upsert flag"),
      opt[String]("eventFormat")
        .action((x, c) => c.copy(eventFormat = x)),
      opt[Unit]("dataTransformation")
        .action((_, c) => c.copy(dataTransformation = true))
        .text("dataTransformation flag"),
      opt[String]("tagFilename")
        .action((x, c) => c.copy(tagFilename = x)),
      opt[Unit]("useFlowMarkers")
        .action((_, c) => c.copy(useFlowMarkers = true))
        .text("useFlowMarkers flag"),
      opt[String]("maxPollRecs")
        .action((x, c) => c.copy(maxPollRecs = x)),
      opt[String]("groupId")
        .action((x, c) => c.copy(groupId = x)),
      opt[String]("clientId")
        .action((x, c) => c.copy(clientId = x)),
      opt[Map[String, String]]("kwargs")
        .valueName("k1=v1,k2=v2...")
        .action((x, c) => c.copy(kwargs = x))
        .text("other arguments"),

    )
  }

  def getFromEnv(name: String): String = {
    val envNamePrefix = "splice.kafkareader."
    val envValue = System.getenv(envNamePrefix + name)
    if (envValue != null) {
      return envValue
    }
    val property = System.getProperty(envNamePrefix + name)
    if (property != null) {
      return property
    }
    null
  }

  def readEnvironment(config: Config) = {
    config.getClass.getDeclaredFields.map(f => {
      f.setAccessible(true)
      val valueType = f.getType
      val typeName = valueType.getSimpleName
      val envValue = getFromEnv(f.getName)
      if (envValue != null) {
        if (typeName.equals("String")) {
          f.set(config, envValue)
        } else if (typeName.equals("int")) {
          f.setInt(config, envValue.toInt)
        } else if (typeName.equals("boolean")) {
          f.setBoolean(config, envValue.equals("true"))
        }
      }
    }
    )
    config
  }

  def parseConfig: Config = {
    // OParser.parse returns Option[Config]
    OParser.parse(parser1, args, Config()) match {
      case Some(config) => {
        // do something
        readEnvironment(config)
        config
      }
      case _ => {
        // arguments are bad, error message will have been displayed
        log.error(s"Command line arguments are bad")
        throw new RuntimeException("Command line arguments are not valid");
      }

    }
  }

  def applicationConfig: Config = parseConfig
}
