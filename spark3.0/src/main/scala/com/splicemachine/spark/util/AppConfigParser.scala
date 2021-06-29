package com.splicemachine.spark.util

import org.apache.log4j.Logger
import scopt.OParser

class AppConfigParser[C](var args: Array[String], var parser: AppConfigCLI[C], var envNamePrefix: String = "splice.kafkareader.") {

  val log = Logger.getLogger(getClass.getName)

  private def getFromEnv(name: String): String = {
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

  private def readEnvironment(config: C) = {
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

  def parseConfig(config: C): C = {
    readEnvironment(config: C)
    OParser.parse(parser.parser, args, config) match {
      case Some(config) => {
        // do something
        config
      }
      case _ => {
        // arguments are bad, error message will have been displayed
        log.error(s"Command line arguments are bad")
        throw new RuntimeException("Command line arguments are not valid");
      }

    }
  }
}
