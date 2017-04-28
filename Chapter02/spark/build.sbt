name := "maths-for-ml-spark"

version := "1.0"

val sparkVersion = "2.0.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)

resolvers ++= Seq(
  "Apache Repository" at "https://repository.apache.org/content/repositories/releases"
)

scalaVersion := "2.11.7"