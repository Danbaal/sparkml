package job

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.feature.{OneHotEncoder, VectorAssembler, StringIndexer}
import org.apache.spark.sql.{DataFrame, SQLContext}
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

  def trainDataPath = getClass.getResource("/adult_train_data.csv").getPath
  def testDataPath = getClass.getResource("/adult_test_data.csv").getPath
  def sch = Schemas.adultSch

  def run(sqlc: SQLContext) = {

    val (trainDF, testDF) = loadData(sqlc)

    val label = "income"
    val indxSuff = "_idx"
    val vecSuff = "_vec"

    val categoricals: Seq[String] = sch.flatMap(sf => if(sf.dataType == StringType) Some(sf.name) else None)
    val doubles: Seq[String] = sch.flatMap(sf => if(sf.dataType == DoubleType) Some(sf.name) else None)

    val indexers: Seq[StringIndexer] = categoricals.map(
      s => new StringIndexer().setInputCol(s).setOutputCol(s+indxSuff)/*.setHandleInvalid("skip")*/)
    val encoders: Seq[OneHotEncoder] = categoricals.flatMap(
      s => if(s == label) None else Some(new OneHotEncoder().setInputCol(s+indxSuff).setOutputCol(s+vecSuff)))

    val colsToAssembler = categoricals.flatMap(s => if(s == label) None else Some(s+vecSuff)).++(doubles).toArray
    val assembler = new VectorAssembler().setInputCols(colsToAssembler).setOutputCol("features")

    val pipeline = new Pipeline().setStages((indexers ++ encoders :+ assembler).toArray)
    val transformer = pipeline.fit(trainDF)
    val featTrainDF = transformer.transform(trainDF)
    val featTestDF = transformer.transform(testDF)

    featTrainDF.printCorrelations(label+indxSuff)

    val featTrainDF_ = featTrainDF.select(col(label+indxSuff).as("label"), col("features"))
    val featTestDF_ = featTestDF.select(col(label+indxSuff).as("label"), col("features"))

    val lr = new LogisticRegression()
    //println("LogisticRegression parameters:\n" + lr.explainParams() + "\n")
    lr.setMaxIter(20)
      .setRegParam(0.01)
      .setElasticNetParam(0.1)

    val lrModel = lr.fit(featTrainDF_)

    val result = lrModel.transform(featTestDF_).cache()

    val valDF = result.select((col("label") === col("prediction")).as("isAMatch")).cache()
    val totalRecords = result.count()
    val matches = valDF.filter(col("isAMatch")).count()

    println("Total records: " + totalRecords)
    println("Total matches: " + matches)
    println("Total mistakes: " + (totalRecords - matches))


    // Extract the summary from the returned LogisticRegressionModel instance
    val lrSummary = lrModel.summary

    // We cast the summary to a BinaryLogisticRegressionSummary since the problem is a
    // binary classification problem.
    val binarySummary = lrSummary.asInstanceOf[BinaryLogisticRegressionSummary]

    // Obtain the areaUnderROC.
    val areaUnderROC = binarySummary.areaUnderROC
    println("Area Under ROC: " + areaUnderROC) // 0.9058589792264902

    // Set the model threshold to maximize F-Measure
    /*val fMeasure = binarySummary.fMeasureByThreshold
    val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
    val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure)
      .select("threshold").head().getDouble(0)
    lrModel.setThreshold(bestThreshold) ??? */

  }

  def loadData(sqlc: SQLContext): (DataFrame, DataFrame) = (loadData(sqlc, trainDataPath), loadData(sqlc, testDataPath))
  def loadData(sqlc: SQLContext, path: String): DataFrame = sqlc.read
    .format("com.databricks.spark.csv")
    .option("nullValue", " ?")
    .load(path)
    .toDF(sch.fieldNames: _*)
    .na.drop()
    .trim()
    .cast(sch)
    .cache()

}
