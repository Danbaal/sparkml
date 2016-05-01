package job

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassifier, BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{VectorIndexer, OneHotEncoder, VectorAssembler, StringIndexer}
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import transform.Featurizer
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

    val (featTrainDF, featTestDF) = Featurizer.transform(trainDF, testDF, label = "income")

    ////////////////////////////////////// LOGISTIC REGRESSION //////////////////////////////////////////////

    val lr = new LogisticRegression()
    //println("LogisticRegression parameters:\n" + lr.explainParams() + "\n")
    lr.setMaxIter(20)
      .setRegParam(0.01)
      .setElasticNetParam(0.1)

    val lrModel = lr.fit(featTrainDF)

    val lrResult = lrModel.transform(featTestDF).cache()

    val totalRecords = lrResult.count()
    val lrMatches = lrResult
      .select((col("label") === col("prediction")).as("isAMatch"))
      .filter(col("isAMatch")).count()

    // Extract the summary from the returned LogisticRegressionModel instance
    val lrSummary = lrModel.summary

    // We cast the summary to a BinaryLogisticRegressionSummary since the problem is a
    // binary classification problem.
    val binarySummary = lrSummary.asInstanceOf[BinaryLogisticRegressionSummary]

    // Obtain the areaUnderROC.
    val lrAreaUnderROC = binarySummary.areaUnderROC

    ////////////////////////////////////// DECISION TREE //////////////////////////////////////////////

    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      //.fit(featTrainDF_)
    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      //.fit(featTrainDF_)

    // Train a DecisionTree model.
    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      // Criterion used for information gain calculation. Slightly better result with 'entropy'
      .setImpurity("entropy")
      // Minimum information gain for a split to be considered at a tree node.
      //.setMinInfoGain(0.01)

    val dtPipe = new Pipeline().setStages(Array(labelIndexer, featureIndexer, dt))

    val dtModel = dtPipe.fit(featTrainDF)

    val dtResult = dtModel.transform(featTestDF).cache()

    val matches = dtResult
      .select((col("label") === col("prediction")).as("isAMatch"))
      .filter(col("isAMatch")).count()

    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setRawPredictionCol("rawPrediction")
      //.setMetricName("areaUnderROC") areaUnderROC is by default

    val dtAreaUnderROC = evaluator.evaluate(dtResult)

    /////////////////////////////////// RESULTS ////////////////////////////////////////////////////

    println("\n\nLogistic Regression result:")
    println("\tTotal records: " + totalRecords) // 15060
    println("\tTotal matches: " + lrMatches) // 12725
    println("\tTotal mistakes: " + (totalRecords - lrMatches)) // 2335
    println("\tArea Under ROC: " + lrAreaUnderROC) // 0.9058589792264902

    println("\nDecision Tree Binary Classifier result:")
    println("\tTotal records: " + totalRecords) // 15060
    println("\tTotal matches: " + matches)  // 12356
    println("\tTotal mistakes: " + (totalRecords - matches)) // 2704
    println("\tArea Under ROC: " + dtAreaUnderROC) // 0.7820159640274078
    println()

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
