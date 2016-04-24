package util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 *
 */
object DFCustomFunctions {
  implicit def addCustomFunctions(df: DataFrame) = new DFCustomFunctions(df)
}

class DFCustomFunctions(df: DataFrame) {

  //import util.DFCustomFunctions._

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

  def cast(sch: StructType) = {
    val newCols = sch map { sf => sf.dataType match {
      case IntegerType => col(sf.name).cast(IntegerType).as(sf.name)
      case DoubleType => col(sf.name).cast(DoubleType).as(sf.name)
      case DateType => col(sf.name).cast(DateType).as(sf.name)
      case BooleanType => col(sf.name).cast(BooleanType).as(sf.name)
      case _ => col(sf.name)
    }}
    df.select(newCols: _*)
  }

}
