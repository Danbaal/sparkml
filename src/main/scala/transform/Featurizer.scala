package transform

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{VectorAssembler, OneHotEncoder, StringIndexer}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType}
import util.DFCustomFunctions._


object Featurizer {

  val indxSuff = "_idx"
  val vecSuff = "_vec"

  def transform(trainDF: DataFrame, testDF: DataFrame, label: String): (DataFrame, DataFrame) = {
    val sch = trainDF.schema

    val categoricals: Seq[String] = sch.flatMap(sf => if (sf.dataType == StringType) Some(sf.name) else None)
    val doubles: Seq[String] = sch.flatMap(sf => if (sf.dataType == DoubleType) Some(sf.name) else None)

    val indexers: Seq[StringIndexer] = categoricals.map(
      s => new StringIndexer().setInputCol(s).setOutputCol(s + indxSuff) /*.setHandleInvalid("skip")*/)
    val encoders: Seq[OneHotEncoder] = categoricals.flatMap(
      s => if (s == label) None else Some(new OneHotEncoder().setInputCol(s + indxSuff).setOutputCol(s + vecSuff)))

    val colsToAssembler = categoricals.flatMap(s => if (s == label) None else Some(s + vecSuff)).++(doubles).toArray
    val assembler = new VectorAssembler().setInputCols(colsToAssembler).setOutputCol("features")

    val pipeline = new Pipeline().setStages((indexers ++ encoders :+ assembler).toArray)
    val transformer = pipeline.fit(trainDF)
    val featTrainDF = transformer.transform(trainDF)
    val featTestDF = transformer.transform(testDF)

    //featTrainDF.printCorrelations(label + indxSuff)
    //featTrainDF.printAllCorrelations()

    (featTrainDF.select(col(label + indxSuff).as("label"), col("features")),
      featTestDF.select(col(label + indxSuff).as("label"), col("features")))
  }

}
