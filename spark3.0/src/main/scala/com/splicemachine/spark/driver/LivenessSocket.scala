package com.splicemachine.spark.driver

import org.apache.log4j.Logger

import java.net.{ServerSocket, Socket}
import java.util.concurrent.{CountDownLatch, ExecutorService, Executors, ScheduledExecutorService, TimeUnit}
import java.io.{BufferedReader, IOException, InputStreamReader}


class LivenessSocket(var port: Int, var updateTimeout: Long) {

  val log: Logger = Logger.getLogger(getClass.getName)

  val livenessExecutor: ExecutorService = Executors.newSingleThreadExecutor
  val statusExecutor: ScheduledExecutorService = Executors.newScheduledThreadPool(2)
  val serverSocket: ServerSocket = new ServerSocket(port)
  val pool = Executors.newFixedThreadPool(2)

  @volatile private var jobFailed = false
  @volatile private var jobLastUpdate: Long = System.currentTimeMillis
  @volatile private var closed = false

  log.info(s"Init Liveness socket listener on port $port")

  livenessExecutor.execute(() => {
    log.info(s"Liveness socket started on port $port")
    while(!closed) {
      val clientSocket: Socket = serverSocket.accept()
      log.info(s"The liveness connection is received from the port ${clientSocket.getPort}");
      pool.execute(new EchoThread(clientSocket))
    }
  })

  statusExecutor.scheduleAtFixedRate(() => {
    if (!closed && (jobFailed || jobLastUpdate + updateTimeout * 1000 < System.currentTimeMillis())) {
      log.warn(s"The pod has to be restarted: Job status $jobFailed, last update: $jobLastUpdate")
      closed = true
      requestRestart()
    }
  }, 5, 5 , TimeUnit.SECONDS);

  def updated(): Unit = {
    jobLastUpdate = System.currentTimeMillis()
  }

  def failed(): Unit = {
    jobFailed = true
    closed = true
    requestRestart()
  }

  def requestRestart(): Unit = {
    serverSocket.close()
  }

  def close(): Unit = {
    serverSocket.close()
    statusExecutor.shutdown()
    livenessExecutor.shutdown()
    pool.shutdown()
  }

  class EchoThread(var socket: Socket) extends Runnable {
    def run(): Unit = {
      try {
        val inp = socket.getInputStream
        val brinp = new BufferedReader(new InputStreamReader(inp))

        while (true) {
          val line = brinp.readLine
          if (line == null) {
            socket.close
            return
          }
        }
      } finally {
        socket.close
      }
    }
  }
}
