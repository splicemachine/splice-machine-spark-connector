package splice

class SpliceOptions(val parameters: Map[String, String]) {
  def url: String = parameters(SpliceOptions.JDBC_URL)
  def table: String = parameters(SpliceOptions.TABLE)
  def kafkaServers: Option[String] = parameters.get(SpliceOptions.KAFKA_SERVERS)
  def kafkaTimeoutMs: Option[String] = parameters.get(SpliceOptions.KAFKA_TIMEOUT_MS)

  assertRequiredOptionsDefined

  /**
    * Asserts that the required parameters (options) were defined:
    * - [[SpliceOptions.JDBC_URL]]
    * - [[SpliceOptions.TABLE]]
    *
    * @throws IllegalStateException required option is not defined
    */
  def assertRequiredOptionsDefined: Unit = {
    if (!parameters.contains(SpliceOptions.JDBC_URL)) {
      throw new IllegalStateException(s"${SpliceOptions.JDBC_URL} option is not defined")
    }
    if (!parameters.contains(SpliceOptions.TABLE)) {
      throw new IllegalStateException(s"${SpliceOptions.TABLE} option is not defined")
    }
  }
}

object SpliceOptions {
  val JDBC_URL = "url"
//  val USER = "user"
//  val PASSWORD = "password"
  val TABLE = "table"
  val KAFKA_SERVERS = "kafkaServers"
  val KAFKA_TIMEOUT_MS = "kafkaTimeout-milliseconds"
//  val JDBC_INTERNAL_QUERIES = "internal"
//  val JDBC_TEMP_DIRECTORY = "tmp"
}
