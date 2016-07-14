name := "sparkml"

version := "1.0"

scalaVersion := "2.10.6"

parallelExecution in Test := false

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "1.6.2",
	"org.apache.spark" %% "spark-sql" % "1.6.2",
	"org.apache.spark" %% "spark-mllib" % "1.6.2",
	"com.databricks" %% "spark-csv" % "1.4.0",
	// Test dependencies
	"org.scalatest" %% "scalatest" % "2.2.1" % "test"
)

javaOptions in Test ++= Seq(s"-Dhadoop.home.dir=${(file(".") / "src" / "main" / "resources" / "hadoop").getAbsolutePath}")

//javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

//mainClass in (Compile, run) := Some("base.job.AdultJob")