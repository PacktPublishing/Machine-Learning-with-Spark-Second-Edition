package org.sparksamples

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by Rajdeep Dua on 6/12/16.
  */
object Util {
  val PATH = "/home/ubuntu/work/spark-2.0.0-bin-hadoop2.7/"
  val DATA_PATH= "../../../data/ml-100k"
  val PATH_MOVIES = DATA_PATH + "/u.item"

  def reduceDimension2(x: Vector) : String= {
    var i = 0
    var l = x.toArray.size
    var l_2 = l/2.toInt
    var x_ = 0.0
    var y_ = 0.0

    for(i <- 0 until l_2) {
      x_ += x(i).toDouble
    }
    for(i <- (l_2 + 1) until l) {
      y_ += x(i).toDouble
    }
    var t = x_ + "," + y_
    return t
  }

  def getMovieDataDF(spark : SparkSession) : DataFrame = {

    //1|Toy Story (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Toy%20Story%20(1995)
    // |0|0|0|1|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0
    val customSchema = StructType(Array(
      StructField("id", StringType, true),
      StructField("name", StringType, true),
      StructField("date", StringType, true),
      StructField("url", StringType, true)));
    val movieDf = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", "|").schema(customSchema)
      .load(PATH_MOVIES)
    return movieDf
  }

}
