name := "scala-spark-app"

version := "1.0"
val sparkVersion = "2.0.0"
scalaVersion := "2.11.7"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)

// https://mvnrepository.com/artifact/org.jblas/jblas
libraryDependencies += "org.jblas" % "jblas" % "1.2.4"
libraryDependencies += "com.github.scopt" % "scopt_2.10" % "3.2.0"


resolvers ++= Seq(
  "Apache Repository" at "https://repository.apache.org/content/repositories/releases"
)
    