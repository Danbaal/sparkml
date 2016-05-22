package job

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
 * Created by Dani on 21/04/2016.
 */
trait SparkApp extends App {

  def name: String

  def run(sqlc: SQLContext): Unit

  def runApp(job: SparkApp = this): Unit = {

    System.setProperty("hadoop.home.dir", getClass.getResource("/hadoop").getPath)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setAppName(name)
      .setMaster("local[*]")
      //.set("spark.sql.shuffle.partitions", "2")
      .set("spark.yarn.executor.memoryOverhead", "2058")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)

    try job.run(sqlc) finally sc.stop()

  }

}
