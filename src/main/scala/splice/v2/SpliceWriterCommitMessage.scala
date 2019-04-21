package splice.v2

import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage

case class SpliceWriterCommitMessage(message: String)
  extends WriterCommitMessage
