package job

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import util.DFUtils._

/**
 * Created by Dani on 21/04/2016.
 */
object AdultJob extends SparkApp {

  def name = "Adult Job"

  val header: Seq[String] = Seq("age", "workclass", "fnlwgt", "education", "education-num", "marital-status", "occupation",
    "relationship", "race", "sex", "capital-gain", "capital-loss", "hours-per-week", "native-country", "target")

  runApp()

  def run(sc: SparkContext, sqlc: SQLContext) = {

    val trainDataPath = getClass.getResource("/adult_data.csv").getPath
    val df = sqlc.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load(trainDataPath)
      .toDF(header: _*)
      .trim() // trim StringType Columns
      .cache()

    df.show()
    df.printSchema()


  }


}
