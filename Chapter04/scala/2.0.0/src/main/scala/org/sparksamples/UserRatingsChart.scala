package org.sparksamples

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import scalax.chart.module.ChartFactories

/**
  * Created by Rajdeep Dua on 2/22/16.
  * Modified on 9/24/16
  */
object UserRatingsChart {

  def main(args: Array[String]) {

    val customSchema = StructType(Array(
      StructField("user_id", IntegerType, true),
      StructField("movie_id", IntegerType, true),
      StructField("rating", IntegerType, true),
      StructField("timestamp", IntegerType, true)))

    val spConfig = (new SparkConf).setMaster("local").setAppName("SparkApp")
    val spark = SparkSession
      .builder()
      .appName("SparkRatingData").config(spConfig)
      .getOrCreate()

    val rating_df = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", "\t").schema(customSchema)
      .load("../../data/ml-100k/u.data")

    val rating_nos_by_user = rating_df.groupBy("user_id").count().sort("count")
    val ds = new org.jfree.data.category.DefaultCategoryDataset
    rating_nos_by_user.show(rating_nos_by_user.collect().length)
    val rating_nos_by_user_collect =rating_nos_by_user.collect()

    var mx = Map(0 -> 0)
    val min = 1
    val max = 1000
    val bins = 100

    val step = (max/bins).toInt
    for (i <- step until (max + step) by step) {
      mx += (i -> 0);
    }
    for( x <- 0 until rating_nos_by_user_collect.length) {
      val user_id = Integer.parseInt(rating_nos_by_user_collect(x)(0).toString)
      val count = Integer.parseInt(rating_nos_by_user_collect(x)(1).toString)
      ds.addValue(count,"Ratings", user_id)
    }
   // ------------------------------------------------------------------

    val chart = ChartFactories.BarChart(ds)
    chart.peer.getCategoryPlot.getDomainAxis().setVisible(false)

    chart.show()
    Util.sc.stop()
  }
}
