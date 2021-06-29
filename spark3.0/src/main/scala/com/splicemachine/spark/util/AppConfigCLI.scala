package com.splicemachine.spark.util

import scopt.OParser

trait AppConfigCLI[C] {
  val parser: OParser[_,C]
}
