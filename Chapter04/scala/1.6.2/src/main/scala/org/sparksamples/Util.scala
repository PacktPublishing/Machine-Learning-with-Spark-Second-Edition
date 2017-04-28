package org.sparksamples

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

/**
  * Created by Rajdeep Dua on 2/2/16.
  */
object Util {
  val PATH = "../../"
  val MOVIE_DATA = PATH + "/data/ml-100k/u.item"
  val spConfig = (new SparkConf).setMaster("local").setAppName("SparkApp")
  val sc = new SparkContext(spConfig)

  def getMovieData() : RDD[String] = {
    val movie_data = sc.textFile(PATH + "/data/ml-100k/u.item")
    return movie_data
  }

  def numMovies() : Long = {
    return getMovieData().count()
  }

  def movieFields() : RDD[Array[String]] = {
    return this.getMovieData().map(lines =>  lines.split("\\|"))
  }

  def mean( x:Array[Int]) : Int = {
    return x.sum/x.length
  }
  def getMovieAges(movie_data : RDD[String]) : scala.collection.Map[Int, Long] = {
    val movie_fields = movie_data.map(lines =>  lines.split("\\|"))
    val years = movie_fields.map( field => field(2)).map( x => convertYear(x))
    val years_filtered = years.filter(x => (x != 1900) )
    val movie_ages = years_filtered.map(yr =>  (1998 - yr) ).countByValue()
    return movie_ages
  }

  def convertYear( x:String) : Int = {
    try
      return x.takeRight(4).toInt
    catch {
      case e: Exception => println("exception caught: " + e + " Returning 1900");
        return 1900
    }
  }

  def getUserData() : RDD[String] = {
    var user_data = Util.sc.textFile(PATH + "/data/ml-100k/u.user")
    return user_data
  }

  def getUserFields() : RDD[Array[String]] = {
    val user_data = this.getUserData()
    val user_fields = user_data.map(l => l.split(","))
    return user_fields
  }
}

