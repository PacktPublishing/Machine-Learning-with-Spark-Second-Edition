name := "scala-spark-app-6"

version := "1.0"

/**
  * Enable 2.0 support
  */
val sparkVersion = "2.0.0"

libraryDependencies ++= Seq(
  //"org.scalanlp" %% "breeze" % "0.12",
  //"org.scalanlp" %% "breeze-natives" % "0.12",
  //"org.scalanlp" %% "breeze-viz" % "0.12",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "com.github.wookietreiber" %% "scala-chart" % "latest.integration",
  "com.itextpdf" % "itextpdf" % "5.5.6",
  "org.jfree" % "jfreesvg" % "3.0",
  "com.databricks" % "spark-csv_2.11" % "1.4.0"
)

resolvers ++= Seq(
  "Apache Repository" at "https://repository.apache.org/content/repositories/releases"
)

scalaVersion := "2.11.7"
