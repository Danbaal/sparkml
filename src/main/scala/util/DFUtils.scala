package util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType


object DFUtils {
  implicit def addCustomFunctions(df: DataFrame) = new DFUtils(df)
}

class DFUtils(df: DataFrame) {

  //import util.DFUtils._

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

}
