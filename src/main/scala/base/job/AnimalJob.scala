package base.job

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorIndexer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, SQLContext}
import base.transform.Featurizer
import base.util.DFCustomFunctions._

object AnimalJob extends SparkApp{

  val name = "Shelter Animal Outcome Job"

  runApp()

  lazy val path = getClass.getResource("/animals/animals-train.csv").getPath
  lazy val sch = null

  def run(sqlc: SQLContext): Unit = {

    val df = loadData(sqlc)
      .select("OutcomeType", "AnimalType","SexuponOutcome")

    //val stringNull = udf((s: String) => if(s == "") "NA" else s)
    //val cols = df.columns map (c => stringNull(col(c)).as(c))
    //val df2 = df.select(cols: _*)

    val (featTrainDF, featTestDF) = Featurizer.transform(df, label = "OutcomeType")

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

    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.impurity, Array("entropy", "gini"))
      .addGrid(rf.numTrees, Array(3, 6))
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

    val result = cvModel.transform(featTestDF)

    println("\nAccuracy: " + result.accuracy()) // 0.6153846153846154

  }

  def loadData(sqlc: SQLContext): DataFrame = sqlc.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("nullValue", "")
    .load(path)
    .selectNot("OutcomeSubtype", "AnimalID", "DateTime")
    //.na.drop()
    //.cast(sch)
    .cache()

}
