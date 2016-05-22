package transform

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{VectorAssembler, OneHotEncoder, StringIndexer}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, DoubleType, StringType}
import util.DFCustomFunctions._

case class Correlation(field1: String, field2: String, correlation: Double){
  override def toString() = s"|||||> The correlation between '$field1' and '$field2' is: ${"%.4f".format(correlation)}"
}

object Featurizer {

  val indxSuff = "_idx"
  val vecSuff = "_vec"

  def transform(trainDF: DataFrame, testDF: DataFrame, label: String): (DataFrame, DataFrame) = {

    val pipeline = createPipeline(trainDF.schema, label)
    val transformer = pipeline.fit(trainDF)
    val featTrainDF = transformer.transform(trainDF)
    val featTestDF = transformer.transform(testDF)

    //featTrainDF.getLabelCorrelations(label + indxSuff).foreach(println)
    //featTrainDF.getFeatureCorrelations(label + indxSuff).foreach(println)

    (featTrainDF.select(col(label + indxSuff).as("label"), col("features")),
      featTestDF.select(col(label + indxSuff).as("label"), col("features")))
  }

  /**
   *
   * @param df
   * @param label
   * @return (trainDF, testDF)
   */
  def transform(df: DataFrame, label: String): (DataFrame, DataFrame) = {

    val pipeline = createPipeline(df.schema, label)
    val transformer = pipeline.fit(df)
    val featDF = transformer.transform(df)

    //featDF.getLabelCorrelations(label + indxSuff).foreach(println)
    //featDF.getFeatureCorrelations(label + indxSuff).foreach(println)

    val splits = featDF
      .select(col(label + indxSuff).as("label"), col("features"))
      .randomSplit(Array(0.70, 0.30), 123L)

    (splits(0), splits(1))
  }

  def createPipeline(sch: StructType, label: String): Pipeline = {
    val categoricals: Seq[String] = sch.filter(_.dataType == StringType).map(_.name)
    val doubles: Seq[String] = sch.filter(_.dataType == DoubleType).map(_.name)

    val indexers: Seq[StringIndexer] = categoricals.map(
      s => new StringIndexer().setInputCol(s).setOutputCol(s + indxSuff) /*.setHandleInvalid("skip")*/)
    val encoders: Seq[OneHotEncoder] = categoricals.filter(_ != label).map(
      s => new OneHotEncoder().setInputCol(s + indxSuff).setOutputCol(s + vecSuff))

    val colsToAssembler = categoricals.filter(_ != label).map(_ + vecSuff).++(doubles).toArray
    val assembler = new VectorAssembler().setInputCols(colsToAssembler).setOutputCol("features")

    new Pipeline().setStages((indexers ++ encoders :+ assembler).toArray)
  }

}
