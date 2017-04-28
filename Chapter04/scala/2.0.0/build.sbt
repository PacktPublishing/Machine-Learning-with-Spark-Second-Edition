name := "chapter04"
version := "1.0"
scalaVersion := "2.11.6"

libraryDependencies += "org.scala-lang" % "scala-compiler" % scalaVersion.value

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.0"

libraryDependencies +="org.scalanlp" %% "breeze-natives" % "0.12"
libraryDependencies +="org.jfree" % "jfreechart" % "1.0.14"
libraryDependencies += "com.github.wookietreiber" %% "scala-chart" % "latest.integration"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.0.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.0.0"

