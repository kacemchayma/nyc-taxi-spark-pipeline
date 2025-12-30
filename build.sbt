name := "spark-taxi-analysis"
version := "1.0"
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-mllib" % "3.5.0"
)

mainClass in (Compile, run) := Some("TaxiAnalysis")
