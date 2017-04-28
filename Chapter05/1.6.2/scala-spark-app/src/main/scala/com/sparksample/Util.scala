package com.sparksample

/**
  * Created by ubuntu on 3/16/16.
  */

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.jblas.DoubleMatrix

/**
  * Created by Rajdeep Dua on 2/2/16.
  */
object Util {
  val PATH = "../.."
  val spConfig = (new SparkConf).setMaster("local").setAppName("SparkApp")
  var sc = new SparkContext(spConfig)

  def getMovieData() : RDD[String] = {
    val movie_data = sc.textFile(PATH + "/data/ml-100k/u.item")
    return movie_data
  }
  def getUserData() : RDD[String] = {
    val user_data = sc.textFile(PATH + "/data/ml-100k/u.data")
    return user_data
  }
  def getDate(): String = {
    val today = Calendar.getInstance().getTime()
    // (2) create a date "formatter" (the date format we want)
    val formatter = new SimpleDateFormat("yyyy-MM-dd-hh.mm.ss")

    // (3) create a new String using the date format we want
    val folderName = formatter.format(today)
    return folderName
  }

  def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }

  def avgPrecisionK(actual: Seq[Int], predicted: Seq[Int], k: Int): Double = {
    val predK = predicted.take(k)
    var score = 0.0
    var numHits = 0.0
    for ((p, i) <- predK.zipWithIndex) {
      if (actual.contains(p)) {
        numHits += 1.0
        score += numHits / (i.toDouble + 1.0)
      }
    }
    if (actual.isEmpty) {
      1.0
    } else {
      score / scala.math.min(actual.size, k).toDouble
    }
  }

}