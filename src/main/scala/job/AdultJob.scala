package job

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import util.DFCustomFunctions._
import util.Schemas
import util.Utils._

/**
 *
 */
object AdultJob extends SparkApp {

  def name = "Adult Job"

  runApp()

  def run(sc: SparkContext, sqlc: SQLContext) = {

    val trainDataPath = getClass.getResource("/adult_train_data.csv").getPath
    val testDataPath = getClass.getResource("/adult_test_data.csv").getPath
    val sch = Schemas.adultSch

    val trainDF = sqlc.read
      .format("com.databricks.spark.csv")
      .option("nullValue", " ?")
      .load(trainDataPath)
      .toDF(sch.fieldNames: _*)
      .na.drop()
      .trim()
      .cast(sch)
      .cache()

    val label = "income"
    val pipeline = getStringFeaturizer(sch, "income")

    val transformer = pipeline.fit(trainDF)

    val finalDF = transformer.transform(trainDF)

    finalDF.printSchema()
    finalDF.show()

    // Having a look to correlations with the target variable //////////////////
    finalDF.schema.map{ sf => if(sf.dataType == StringType || sf.name == "features") "Not Supported" else {
      val colName = sf.name
      val corr = finalDF.stat.corr(label, colName)
      s"||| The correlation between 'income' and $colName is: $corr"
    }}.foreach(println)
    ////////////////////////////////////////////////////////////////////////////

  }

}
