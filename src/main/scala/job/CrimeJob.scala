package job

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._

/**
 * Created by Dani on 02/05/2016.
 */
object CrimeJob /*extends SparkApp*/{

  val name = "Crime Job"

  //runApp()

  lazy val path = getClass.getResource("/crime-train.csv").getPath

  def run(sqlc: SQLContext) = {
    import sqlc.implicits._

    val df = sqlc.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(path)
      .cache()

    //df.select("Address").take(40).foreach(println)
    df.describe("X", "Y").show()
    df.select($"X").show()

  }
}
