package base.job

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorIndexer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import base.transform.Featurizer
import base.util.Schemas
import base.util.DFCustomFunctions._


object CrimeJob /*extends SparkApp*/{

  val name = "Crime Job"

  //runApp()

  lazy val path = getClass.getResource("/crime/crime-train.csv").getPath
  lazy val sch = Schemas.crimeSch

  /*
    This dataset contains incidents derived from SFPD Crime Incident Reporting system.
    The data ranges from 1/1/2003 to 5/13/2015. The training set and test set rotate every week,
    meaning week 1,3,5,7... belong to test set, week 2,4,6,8 belong to training set.
  */

  def run(sqlc: SQLContext) = {

    val df = loadData(sqlc)

    val (featTrainDF, featTestDF) = transform(sqlc, df)

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")

    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")

    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, rf))

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // this grid will have 3 x 3 = 9 parameter settings for CrossValidator to choose from.
    val paramGrid = new ParamGridBuilder()
      //.addGrid(rf.impurity, Array("entropy", "gini"))
      .addGrid(rf.numTrees, Array(1, 3, 6))
      .addGrid(rf.minInfoGain, Array(0.001, 0.005, 0.01))
      .build()

    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator().setLabelCol("indexedLabel"))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)

    // Run cross-validation, and choose the best set of parameters.
    val cvModel = cv.fit(featTrainDF)

    println("Average Metrics: " + cvModel.avgMetrics)

    /* TODO: Getting: org.apache.spark.SparkException: Job aborted due to stage failure:
    Task 38 in stage 46.0 failed 1 times, most recent failure: Lost task 38.0 in stage 46.0
    (TID 2380, localhost): ExecutorLostFailure (executor driver exited caused by one of the running tasks)
    Reason: Executor heartbeat timed out after 131654 ms

    related issue: http://stackoverflow.com/questions/29566522/spark-executor-lost-failure

    Not able to fix the issue increasing memory overhead as usually does when loosing executors in cluster mode.
    Possible cause: Getting to large Vectors for categorical variables
    Next steps: Play with tunning parameters of spark related with memory.
      try java option: options(java.parameters = "-Xmx4000m")
      try maxCategories for vector indexer

    I will come back to this!
    */

  }

  def transform(sqlc: SQLContext, df: DataFrame): (DataFrame, DataFrame) = {

    //println("Distinct Address: " + df.select("Address").distinct().count())

    val cleanAddress = udf((str: String) => "\\d+ Block of ".r.replaceAllIn(str, ""))

    val df2 = df.withColumn("newAddress", cleanAddress(col("Address"))).cache()

    //println("Distinct Address After cleaning: " + df2.distinct().count())

    Featurizer.transform(df2, label = "Category")
  }

  def loadData(sqlc : SQLContext) = sqlc.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .load(path)
    .sample(false, .01, 321L)
    .repartition(100)
    .selectNot("Dates", "Descript", "Resolution")
    .cast(sch)
    .cache()
}
