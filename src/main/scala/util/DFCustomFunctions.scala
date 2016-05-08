package util

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

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
    val newCols = sch flatMap { sf => (sf.dataType, df.columns.contains(sf.name)) match {
      case (IntegerType, true)  => Some(col(sf.name).cast(IntegerType).as(sf.name))
      case (DoubleType, true)   => Some(col(sf.name).cast(DoubleType).as(sf.name))
      case (DateType, true)     => Some(col(sf.name).cast(DateType).as(sf.name))
      case (BooleanType, true)  => Some(col(sf.name).cast(BooleanType).as(sf.name))
      case (_, true)            => Some(col(sf.name))
      case (_, false)           => None
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
  def printCorrelations(label: String): Unit =
    df.schema.flatMap { sf => sf.dataType match {
      case DoubleType => {
        val corr = df.stat.corr(label, sf.name)
        Some(s"// The correlation between '$label' and '${sf.name}' is: $corr")
      }
      case _ => None
    }}.foreach(println)

  def printAllCorrelations(): Unit = {
    val fieldCombinations = df.schema
      .flatMap( sf => if(sf.dataType == DoubleType) Some(sf.name) else None).toSet
      .subsets(2).map(_.toSeq).toSeq
    val seqS: Seq[String] = fieldCombinations map (
      pair => s"// The correlation between '${pair(0)}' and '${pair(1)}' is: ${df.stat.corr(pair(0), pair(1))}")
    seqS.foreach(println)
  }

  def accuracy(label: String = "label", prediction: String = "prediction"): Double = {
    val totalRecords = df.count()
    df.select((col(label) === col(prediction)).as("isAMatch"))
      .filter(col("isAMatch")).count().toDouble / totalRecords
  }

}
