package job

import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, VectorAssembler, StringIndexer}
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
    val indxSuff = "_idx"
    val vecSuff = "_vec"
    val categoricals: Seq[String] = sch.flatMap(sf => if(sf.dataType == StringType) Some(sf.name) else None)
    val indexers: Seq[StringIndexer] = categoricals.map(s => new StringIndexer().setInputCol(s).setOutputCol(s+indxSuff))
    val encoders: Seq[OneHotEncoder] = categoricals.flatMap(s => if(s == label) None else Some(new OneHotEncoder().setInputCol(s+indxSuff).setOutputCol(s+vecSuff)))

    val pipeline = new Pipeline().setStages(indexers ++ encoders toArray)
    val transformer = pipeline.fit(trainDF)
    val vectorizedDF = transformer.transform(trainDF)

    vectorizedDF.show()

    //TODO: Use Vector Assembler to group all features

    // Having a look to correlations with the target variable ////////////////////////////////
    val doubles = sch.flatMap(sf => if(sf.dataType == DoubleType) Some(sf.name) else None)
    categoricals.map{ s => {
      val corr = vectorizedDF.stat.corr(label+indxSuff, s+indxSuff)
      s"// The correlation between 'income' and '$s' is: $corr"
    }}.foreach(println)
    doubles.map{ s => {
      val corr = vectorizedDF.stat.corr(label+indxSuff, s)
      s"// The correlation between 'income' and '$s' is: $corr"
    }}.foreach(println)
    //////////////////////////////////////////////////////////////////////////////////////////
    // The correlation between 'income' and 'workclass' is: 0.13693664382909257
    // The correlation between 'income' and 'education' is: 0.046115697656336295
    // The correlation between 'income' and 'marital-status' is: -0.31335949435820704
    // The correlation between 'income' and 'occupation' is: -0.18294820906250017
    // The correlation between 'income' and 'relationship' is: -0.25524058953146234
    // The correlation between 'income' and 'race' is: -0.06790255449005365
    // The correlation between 'income' and 'sex' is: -0.21669868107558513
    // The correlation between 'income' and 'native-country' is: -0.019982702251129778
    // The correlation between 'income' and 'income' is: 1.0
    // The correlation between 'income' and 'age' is: 0.2419981362661187
    // The correlation between 'income' and 'fnlwgt' is: -0.00895742335917164
    // The correlation between 'income' and 'education-num' is: 0.3352861967526381
    // The correlation between 'income' and 'capital-gain' is: 0.22119621454805571
    // The correlation between 'income' and 'capital-loss' is: 0.15005330839729833
    // The correlation between 'income' and 'hours-per-week' is: 0.2294801298885108
    //////////////////////////////////////////////////////////////////////////////////////////

  }

}
