import com.splicemachine.EngineDriver
import com.splicemachine.db.jdbc.ClientBaseDataSource
import com.splicemachine.derby.impl.SpliceSpark
import com.splicemachine.spark.splicemachine.SplicemachineContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class SpliceSpec extends FlatSpec
  with Matchers
  with BeforeAndAfter {

  val traceLevel = ClientBaseDataSource.TRACE_ALL
  val url = s"jdbc:splice://localhost:1527/splicedb;user=splice;password=admin;traceLevel=$traceLevel"

  var spliceCtx: SplicemachineContext = _

  "Splice database" should "be up and running" in {
    val dbVer = EngineDriver.driver().getVersion
    val release = dbVer.getRelease
    release should startWith("2.8.0")
  }

  it should "allow inserting a Spark dataset" in {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.ui.enabled", false)
      .getOrCreate
    import spark.implicits._

    SpliceSpark.setContext(spark.sparkContext)

    val testName = this.getClass.getSimpleName

    val tableName = testName
    val data = Seq((0L, testName)).toDF("id", "test_name")

    if (spliceCtx.tableExists(tableName)) {
      spliceCtx.dropTable(tableName)
    }
    spliceCtx.createTable(tableName, data.schema, keys = Seq.empty, createTableOptions = "UNUSED")
    spliceCtx.insert(data, tableName)
  }

  before {
    spliceCtx = new SplicemachineContext(url)
  }
}