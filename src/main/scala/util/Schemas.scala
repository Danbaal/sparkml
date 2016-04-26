package util

import org.apache.spark.sql.types._

/**
 *
 */
object Schemas {

  val adultSch = StructType(Seq(
    StructField("age", DoubleType, true),
    StructField("workclass", StringType, true),
    StructField("fnlwgt", DoubleType, true),
    StructField("education", StringType, true),
    StructField("education-num", DoubleType, true),
    StructField("marital-status", StringType, true),
    StructField("occupation", StringType, true),
    StructField("relationship", StringType, true),
    StructField("race", StringType, true),
    StructField("sex", StringType, true),
    StructField("capital-gain", DoubleType, true),
    StructField("capital-loss", DoubleType, true),
    StructField("hours-per-week", DoubleType, true),
    StructField("native-country", StringType, true),
    StructField("income", StringType, true)))

}
