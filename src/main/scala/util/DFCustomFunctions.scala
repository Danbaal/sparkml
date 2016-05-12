package util

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import transform.Correlation

/**
 *
 */
object DFCustomFunctions {
  implicit def addCustomFunctions(df: DataFrame) = new DFCustomFunctions(df)
}

class DFCustomFunctions(df: DataFrame) {

  /**
   * Method to trim StringType Columns
   * @return
   */
  def trim(): DataFrame = {
    val newCols = df.schema.map(s => s.dataType match {
      case StringType => org.apache.spark.sql.functions.trim(col(s.name)).as(s.name)
      case _ => col(s.name)
    })
    df.select(newCols: _*)
  }

  /**
   *
   * @param sch
   * @return
   */
  def cast(sch: StructType) = {
    val cols = df.columns
    val newCols = sch flatMap { sf => (sf.dataType == StringType, cols.contains(sf.name)) match {
      case (false, true) => Some(col(sf.name).cast(sf.dataType).as(sf.name))
      case (true, true) => Some(col(sf.name))
      case (_, false) => None
    }}
    df.select(newCols: _*)
  }

  /**
   *
   * @param fieldsToDrop
   * @return
   */
  def selectNot(fieldsToDrop: String*) = {
    val newCols = df.columns flatMap (f => if(fieldsToDrop.contains(f)) None else Some(col(f)))
    df.select(newCols: _*)
  }

  /**
   *
   * @param label
   */
  def getLabelCorrelations(label: String = "label"): Seq[Correlation] =
    df.schema.flatMap { sf => sf.dataType match {
      case DoubleType => {
        val corr = df.stat.corr(label, sf.name)
        Some(Correlation(label, sf.name, corr))
      }
      case _ => None
    }}

  /**
   *
   * @param label
   * @return
   */
  def getFeatureCorrelations(label: String = "label"): Seq[Correlation] = {
    val fieldCombinations = df.schema
      .flatMap( sf => if(sf.dataType == DoubleType && sf.name != label) Some(sf.name) else None)
      .toSet.subsets(2).map(_.toSeq).toSeq
    fieldCombinations map ( pair => Correlation(pair(0), pair(1), df.stat.corr(pair(0), pair(1))) )
  }

  /**
   *
   * @param label
   * @return
   */
  def getAllCorrelations(label: String = "label"): Seq[Correlation] =
    getLabelCorrelations(label) ++ getFeatureCorrelations(label)

  /**
   *
   * @param label
   * @param prediction
   * @return
   */
  def accuracy(label: String = "label", prediction: String = "prediction"): Double = {
    val totalRecords = df.count()
    df.select((col(label) === col(prediction)).as("isAMatch"))
      .filter(col("isAMatch")).count().toDouble / totalRecords
  }

}