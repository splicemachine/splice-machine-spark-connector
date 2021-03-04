import com.splicemachine.spark2.splicemachine.SplicemachineContext

package object splice {

  def getSplicemachineContext(opts: SpliceOptions): SplicemachineContext =
    (opts.url, opts.kafkaServers, opts.kafkaTimeoutMs) match {
      case (url, Some(servers), None) => new SplicemachineContext(url, servers)
      case (url, None, None) => new SplicemachineContext(url)
      case (url, Some(servers), Some(timeout)) => new SplicemachineContext(url, servers, timeout.toLong)
      case (url, None, Some(timeout)) => new SplicemachineContext(url, kafkaPollTimeout=timeout.toLong)
    }

    //
//  def check(str: String, ostr: Option[String], lstr: Option[String]): Unit = {
//    (str, ostr, lstr) match {
//      case (url, Some(servers), None) => println("1 " + url + servers)
//      case (url, None, None) => println("2 " + url)
//      case (url, Some(servers), Some(timeout)) => println("3 " + url + servers + timeout.toLong.toString)
//      case (url, None, Some(timeout)) => println("4 " + url + timeout.toLong.toString)
//    }
//  }
}
