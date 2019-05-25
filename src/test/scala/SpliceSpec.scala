import com.splicemachine.EngineDriver
import com.splicemachine.spark.splicemachine.SplicemachineContext
import org.scalatest.{FlatSpec, Matchers}

class SpliceSpec extends FlatSpec
  with Matchers {

  "Splice database" should "be up and running" in {

    val url = "jdbc:splice://:1527/splicedb;user=splice;password=admin"
    val spliceCtx = new SplicemachineContext(url)

    spliceCtx should not be null

    val dbVer = EngineDriver.driver().getVersion
    val release = dbVer.getRelease
    release should startWith ("2.8.0")
  }
}