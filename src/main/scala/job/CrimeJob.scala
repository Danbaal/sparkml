package job

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._

/**
 * Created by Dani on 02/05/2016.
 */
object CrimeJob extends SparkApp{

  val name = "Crime Job"

  runApp()

  lazy val path = getClass.getResource("/crime-train.csv").getPath

  /*
    This dataset contains incidents derived from SFPD Crime Incident Reporting system.
    The data ranges from 1/1/2003 to 5/13/2015. The training set and test set rotate every week,
    meaning week 1,3,5,7... belong to test set, week 2,4,6,8 belong to training set.
  */

  def run(sqlc: SQLContext) = {
    import sqlc.implicits._

    val df = sqlc.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(path)
      .cache()

    println("Distinct Address: " + df.select("Address").distinct().count())
    //df.select("Address").distinct().count()
    //df.describe("X", "Y").show()

    val cleanAddress = udf((str: String) => "\\d+ Block of ".r.replaceAllIn(str, ""))

    val df2 = df.select(cleanAddress($"Address").as("newAddress")).cache()

    println("Distinct Address After cleaning: " + df2.distinct().count())

  }
}
