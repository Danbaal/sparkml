package job

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{NaiveBayes, DecisionTreeClassifier, LogisticRegression}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{VectorIndexer, StringIndexer}
import org.apache.spark.sql.{DataFrame, SQLContext}
import transform.Featurizer
import util.DFCustomFunctions._
import util.Schemas

/**
 *
 */
object AdultJob extends SparkApp {

  val name = "Adult Job"

  runApp()

  lazy val trainDataPath = getClass.getResource("/adult/adult_train_data.csv").getPath
  lazy val testDataPath = getClass.getResource("/adult/adult_test_data.csv").getPath
  lazy val sch = Schemas.adultSch

  def run(sqlc: SQLContext) = {

    val (trainDF, testDF) = loadData(sqlc)

    val (featTrainDF, featTestDF) = Featurizer.transform(trainDF, testDF, label = "income")

    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
    //.setMetricName("areaUnderROC") areaUnderROC is already by default

    ////////////////////////////////////// LOGISTIC REGRESSION //////////////////////////////////////////////

    val lr = new LogisticRegression()

    val lrModel = lr.fit(featTrainDF)
    val lrResult = lrModel.transform(featTestDF).cache()

    val lrAccuracy = lrResult.accuracy()
    val lrAreaUnderROC = evaluator.evaluate(lrResult)


    ////////////////////////////////////// NAIVE BAYES //////////////////////////////////////////////

    val nb = new NaiveBayes()

    val nbModel = nb.fit(featTrainDF)
    val nbResult = nbModel.transform(featTestDF)

    val nbAccuracy = nbResult.accuracy()
    val nbAreaUnderROC = evaluator.evaluate(nbResult)


    ////////////////////////////////////// DECISION TREE //////////////////////////////////////////////

    // Index labels, adding metadata to the label column.
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")

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

    val dtAccuracy = dtResult.accuracy()
    val dtAreaUnderROC = evaluator.setLabelCol("indexedLabel").evaluate(dtResult)


    /////////////////////////////////// RESULTS ////////////////////////////////////////////////////

    println("\n\nLogistic Regression result:")
    println("\tAccuracy: " + lrAccuracy) // 0.847
    println("\tArea Under ROC: " + lrAreaUnderROC) // 0.903

    println("\nNaive Bayes result:")
    println("\tAccuracy: " + nbAccuracy) // 0.775
    println("\tArea Under ROC: " + nbAreaUnderROC) // 0.221

    println("\nDecision Tree Classifier result:")
    println("\tAccuracy: " + dtAccuracy) // 0.820
    println("\tArea Under ROC: " + dtAreaUnderROC) // 0.795
    println()

  }

  def loadData(sqlc: SQLContext): (DataFrame, DataFrame) =
    (loadData(sqlc, trainDataPath), loadData(sqlc, testDataPath))

  def loadData(sqlc: SQLContext, path: String): DataFrame = sqlc.read
    .format("com.databricks.spark.csv")
    .option("nullValue", " ?")
    .load(path)
    .toDF(sch.fieldNames: _*)
    //These fields have very low correlation with 'income' (under 0.02) and removing them barely affects final results
    .selectNot("fnlwgt", "native-country")
    .na.drop() // Removing records with unknowns
    .trim()
    .cast(sch)
    .cache()

}
