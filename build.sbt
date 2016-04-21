name := "sparkml"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "1.6.1",
	"org.apache.spark" %% "spark-sql" % "1.6.1",
	"org.apache.spark" %% "spark-mllib" % "1.6.1",
	"com.databricks" %% "spark-csv" % "1.4.0")
