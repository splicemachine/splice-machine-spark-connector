//import com.splicemachine.EngineDriver
import com.splicemachine.db.jdbc.ClientBaseDataSource
//import com.splicemachine.derby.impl.SpliceSpark
import com.splicemachine.spark2.splicemachine.SplicemachineContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class SpliceSpec extends FlatSpec
  with Matchers
  with BeforeAndAfter {

  val traceLevel = ClientBaseDataSource.TRACE_ALL
  val url = s"jdbc:splice://localhost:1527/splicedb;user=splice;password=admin;traceLevel=$traceLevel"

  val testName = this.getClass.getSimpleName
  val tableName = testName

  var spliceCtx: SplicemachineContext = _

  "Splice database" should "be up and running" in {
    spliceCtx.tableExists("sys.systables")
  }

  it should "read a dataset from a table" in {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.ui.enabled", false)
      .getOrCreate
    import spark.implicits._

    val data = Seq((0L, testName)).toDF("ID", "TEST_NAME")
    if (!spliceCtx.tableExists(tableName)) {
      spliceCtx.createTable(tableName, data.schema, keys = Seq.empty, createTableOptions = "UNUSED")
    }

    spliceCtx.rdd(tableName).collect().toSeq.foreach(println)
  }

  it should "allow inserting a Spark dataset" in {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.ui.enabled", false)
      .getOrCreate
    import spark.implicits._

//    SpliceSpark.setContext(spark.sparkContext)

    // FIXME Splice supports dataframes with uppercase column names only
    val data = Seq((0L, testName)).toDF("ID", "TEST_NAME")

    if (spliceCtx.tableExists(tableName)) {
      spliceCtx.dropTable(tableName)
    }
    spliceCtx.createTable(tableName, data.schema, keys = Seq.empty, createTableOptions = "UNUSED")
    spliceCtx.insert(data, tableName)
  }

  before {
    SparkSession
      .builder()
      .master("local[*]")
      .config("spark.ui.enabled", false)
      .getOrCreate
    spliceCtx = new SplicemachineContext(url)
  }
}