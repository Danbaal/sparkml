package job

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

/**
 * Created by Dani on 21/04/2016.
 */
trait SparkApp extends App {

  def name: String

  def run(sc: SparkContext, sqlc: SQLContext): Unit

  def runApp(job: SparkApp = this): Unit = {

    val conf = new SparkConf().setAppName(name).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)

    try job.run(sc, sqlc) finally sc.stop()

  }

}
