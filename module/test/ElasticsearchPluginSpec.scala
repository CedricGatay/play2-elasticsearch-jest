import com.codetroopers.play.elasticsearch.{IndexService, IndexClient}
import org.specs2.mutable.Specification
import play.api.test.Helpers._

/**
 * Base tests for the Elasticsearch Plugin
 */
class ElasticsearchPluginSpec extends Specification with ElasticsearchTestHelper {

  sequential

  "ElasticsearchPlugin" should {
    "provide an elasticsearch client on start" in {
      running(esFakeApp) {
        waitForYellowStatus()
        IndexClient.client must not beNull
      }
    }
    "provide an elasticsearch node on start" in {
      running(esFakeApp) {
        waitForYellowStatus()
        IndexClient.node must not beNull
      }
    }
    "load custom settings on local node " in {
      running(esFakeApp) {
        waitForYellowStatus()
        IndexClient.node.settings().get("cluster.name") must beEqualTo("play2-elasticsearch")
      }
    }
    "create the index on start" in {
      running(esFakeApp) {
        waitForYellowStatus()
        IndexService.existsIndex("test-index1") must beTrue
      }
    }
    "allow deleting an index" in {
      running(esFakeApp) {
        waitForYellowStatus()
        IndexService.deleteIndex("test-index1")
        IndexService.existsIndex("test-index1") must beFalse
      }
    }
    /*
    TODO: Check why this test fail on travis and not locally
    "create configured mapping" in {
      running(esFakeApp) {
        waitForYellowStatus()
        IndexService.getMapping("test-index1","testType") must beEqualTo(testMapping.get("testType"))
      }
    }
    */
  }

}