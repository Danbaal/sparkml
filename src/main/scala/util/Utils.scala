package util

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.sql.types.{StringType, StructType}

/**
 *
 */
object Utils {

  /**
   *
   * @param schema
   * @param label
   * @return
   */
  def getStringFeaturizer(schema: StructType, label: String): Pipeline = {

    def isLabel(name: String, suffix: String) = if (name == label) "label" else name + suffix

    val colsAndIndexers: Map[String, Option[StringIndexer]] = schema.map { sf => sf.dataType match {
      case StringType => isLabel(sf.name, "F") ->
        Some(new StringIndexer()
          .setInputCol(sf.name)
          .setOutputCol(isLabel(sf.name, "F")))
      case _ => isLabel(sf.name, "") -> None
    }} toMap

    val colsToAssembler: Array[String] = colsAndIndexers.keySet.filterNot(_ == "label").toArray
    val indexers: Seq[StringIndexer] = colsAndIndexers.values.toSeq.flatten

    val assembler = new VectorAssembler()
      .setInputCols(colsToAssembler)
      .setOutputCol("features")

    new Pipeline().setStages(Array((indexers :+ assembler): _*))
  }

}
