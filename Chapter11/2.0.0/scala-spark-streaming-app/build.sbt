name := "scala-spark-streaming-app-11"
version := "1.0"
scalaVersion := "2.11.7"

val sparkVersion = "2.0.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.jfree" % "jfreechart" % "1.0.14",
  "com.github.wookietreiber" % "scala-chart_2.11" % "0.5.0",
  "org.apache.spark" %% "spark-streaming" % sparkVersion
)


    