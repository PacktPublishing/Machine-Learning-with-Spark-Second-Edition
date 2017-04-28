name := "scala-spark-app-7"
version := "1.0"
scalaVersion := "2.11.7"

val sparkVersion = "2.0.0"

libraryDependencies ++= Seq(
//  "org.scalanlp" %% "breeze" % "0.12",
//  "org.scalanlp" %% "breeze-natives" % "0.12",
//  "org.scalanlp" %% "breeze-viz" % "0.12",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.jfree" % "jfreechart" % "1.0.14",
  "com.github.wookietreiber" % "scala-chart_2.11" % "0.5.0"
//  "org.scalanlp" % "breeze_2.11" % "0.12",
//  "org.scalanlp" % "breeze-viz_2.11" % "0.12"
)

////libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.6.1"
//libraryDependencies +=  "org.scalanlp" %% "breeze" % "0.12"
//libraryDependencies +=  "org.scalanlp" %% "breeze-natives" % "0.12"
////libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "1.6.1"
//libraryDependencies +="org.jfree" % "jfreechart" % "1.0.14"
//libraryDependencies += "com.github.wookietreiber" % "scala-chart_2.11" % "0.5.0"
//libraryDependencies += "org.scalanlp" % "breeze_2.11" % "0.12"
//libraryDependencies += "org.scalanlp" % "breeze-viz_2.11" % "0.12"

resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
