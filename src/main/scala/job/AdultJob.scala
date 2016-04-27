package job

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{OneHotEncoder, VectorAssembler, StringIndexer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import util.DFCustomFunctions._
import util.Schemas

/**
 *
 */
object AdultJob extends SparkApp {

  def name = "Adult Job"

  runApp()

  def run(sqlc: SQLContext) = {

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
    val doubles = sch.flatMap(sf => if(sf.dataType == DoubleType) Some(sf.name) else None)

    val indexers: Seq[StringIndexer] = categoricals.map(
      s => new StringIndexer().setInputCol(s).setOutputCol(s+indxSuff))
    val encoders: Seq[OneHotEncoder] = categoricals.flatMap(
      s => if(s == label) None else Some(new OneHotEncoder().setInputCol(s+indxSuff).setOutputCol(s+vecSuff)))

    val colsToAssembler = categoricals.flatMap(s => if(s == label) None else Some(s+vecSuff)) ++ doubles toArray
    val assembler = new VectorAssembler().setInputCols(colsToAssembler).setOutputCol("features")

    val pipeline = new Pipeline().setStages(indexers ++ encoders :+ assembler toArray)
    val transformer = pipeline.fit(trainDF)
    val featuredDF = transformer.transform(trainDF)

    //featuredDF.show()

    val df = featuredDF.select(col(label+indxSuff).as("label"), col("features"))

    val lr = new LogisticRegression()
    //println("LogisticRegression parameters:\n" + lr.explainParams() + "\n")
    lr.setMaxIter(20)
      .setRegParam(0.01)

    val lrModel = lr.fit(df)

    val result = lrModel.transform(df).cache() // TODO: Do it with test Data!

    val valDF = result.select((col("label") === col("prediction")).as("isAMatch"))
    val totalRecords = result.count()
    val matches = valDF.filter(col("isAMatch")).count()

    println("Total records: " + totalRecords)
    println("Total matches: " + matches)
    println("Total mistakes: " + (totalRecords - matches))

    //Total records: 30162
    //Total matches: 25569
    //Total mistakes: 4593


    //result.show()

    // Having a look to correlations with the target variable ////////////////////////////////
    //categoricals.map{ s => {
    //  val corr = featuredDF.stat.corr(label+indxSuff, s+indxSuff)
    //  s"// The correlation between 'income' and '$s' is: $corr"
    //}}.foreach(println)
    //doubles.map{ s => {
    //  val corr = featuredDF.stat.corr(label+indxSuff, s)
    //  s"// The correlation between 'income' and '$s' is: $corr"
    //}}.foreach(println)
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
