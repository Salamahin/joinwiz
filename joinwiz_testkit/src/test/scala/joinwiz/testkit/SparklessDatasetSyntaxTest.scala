//package joinwiz.testkit
//
//import joinwiz.DatasetSyntaxTest
//import org.scalatest.funsuite.AnyFunSuite
//import org.scalatest.matchers.should.Matchers
//
//class SparklessDatasetSyntaxTest extends AnyFunSuite with DatasetSyntaxTest with Matchers {
//
//  test("has dataset-like syntax") {
//    import joinwiz.testkit.implicits._
//    testee(as, bs) should contain only ("duplicated", "unique")
//  }
//}
