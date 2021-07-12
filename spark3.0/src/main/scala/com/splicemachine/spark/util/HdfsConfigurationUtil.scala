package com.splicemachine.spark.util

import org.apache.hadoop.conf.Configuration

/**
 * The util class fills the Hadoop Configuration with the values, which are needed to enable access the hdfs file
 * system via URLs, like hdfs://hdfs/tmp/
 */
object HdfsConfigurationUtil {
  def setHdfsConfig(conf: Configuration, fsName: String, nameNodes: String, nameNodeRPCs: String): Unit = {
    setHdfsConfig(conf: Configuration, fsName: String, nameNodes.split(";"), nameNodeRPCs.split(";"))
  }

  def setHdfsConfig(conf: Configuration, fsName: String, nameNodes: Array[String], nameNodeRPCs: Array[String]) {
    if (nameNodes.length != nameNodeRPCs.length) {
      throw new IllegalArgumentException(s"The number of nameNodes does not correspond to nameNodeRPCs: ${nameNodes.length} / ${nameNodeRPCs.length}")
    }
    conf.set("fs.defaultFS", s"hdfs://$fsName")
    conf.set("fs.default.name", s"hdfs://$fsName")
    conf.set("dfs.nameservices",fsName)
    conf.set("dfs.nameservice.id",fsName)
    conf.set(s"dfs.ha.namenodes.$fsName",nameNodes.mkString(","))
    for (i <- 0 until nameNodes.length) {
      conf.set(s"dfs.namenode.rpc-address.$fsName.${nameNodes(i)}",nameNodeRPCs(i))
    }
    conf.set("dfs.client.failover.proxy.provider.hdfs", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
  }

}
