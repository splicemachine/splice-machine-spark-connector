package com.splicemachine.spark.driver

import org.apache.log4j.Logger

import java.net.{ServerSocket, Socket}
import java.util.concurrent.{ExecutorService, Executors}
import java.io.{BufferedReader, IOException, InputStreamReader}

class LivenessSocket(var port: Int) {

  val log: Logger = Logger.getLogger(getClass.getName)

  val livenessExecutor: ExecutorService = Executors.newSingleThreadExecutor
  val serverSocket: ServerSocket = new ServerSocket(port)
  val pool = Executors.newFixedThreadPool(2)

  log.info(s"Init Liveness socket listener on port $port")

  livenessExecutor.execute(() => {
    log.info(s"Liveness socket started on port $port")
    while(true) {
      val clientSocket: Socket = serverSocket.accept()
      log.info(s"The liveness connection is received from the port ${clientSocket.getPort}");
      pool.execute(new EchoThread(clientSocket))
    }
  })

  def close(): Unit = {
    livenessExecutor.shutdown()
    pool.shutdown()
    serverSocket.close()
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
