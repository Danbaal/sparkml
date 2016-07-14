package base.util

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

  val crimeSch = StructType(Seq(
    StructField("Dates", TimestampType, true),
    StructField("Category", StringType, true), // Label
    StructField("Descript", StringType, true), // Only in train.
    StructField("DayOfWeek", StringType, true),
    StructField("PdDistrict", StringType, true),
    StructField("Resolution", StringType, true), // Only in train
    StructField("Address", StringType, true),
    StructField("X", DoubleType, true),
    StructField("Y", StringType, true)))

  val animalSch = StructType(Seq(
    StructField("AnimalID", StringType, true),
    StructField("Name", StringType, true),
    StructField("DateTime", TimestampType, true),
    StructField("OutcomeType", StringType, true),
    StructField("OutcomeSubtype", StringType, true),
    StructField("AnimalType", StringType, true),
    StructField("SexuponOutcome", StringType, true),
    StructField("AgeuponOutcome", StringType, true),
    StructField("Breed", StringType, true),
    StructField("Color", StringType, true)))

}
