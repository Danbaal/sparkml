package base

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

trait SharedSparkTestBase extends FunSuite with Matchers with BeforeAndAfterAll {

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Test")
    .set("spark.ui.enabled", "false")
    .set("spark.yarn.executor.memoryOverhead", "512")

  private var _sc: SparkContext = _
  private var _sqlc: SQLContext = _

  def sc: SparkContext = _sc
  def sqlc: SQLContext = _sqlc

  override def beforeAll() {
    _sc = new SparkContext(conf)
    _sqlc = new SQLContext(sc)
    super.beforeAll()
  }

  override def afterAll() = {
    try _sc.stop() finally super.afterAll()
  }

}
