package org.sparksamples

import java.awt.Font

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.jfree.chart.axis.CategoryLabelPositions

import scalax.chart.module.ChartFactories

/**
  * Created by Rajdeep Dua on 2/22/16.
  */
object CountByRatingChart {

  def main(args: Array[String]) {
    /*val rating_data_raw = Util.sc.textFile("../../data/ml-100k/u.data")
    val rating_data = rating_data_raw.map(line => line.split("\t"))
    val ratings = rating_data.map(fields => fields(2).toInt)
    val ratings_count = ratings.countByValue()*/

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

    val rating_df_count = rating_df.groupBy("rating").count().sort("rating")
    //val rating_df_count_sorted = rating_df_count.sort("count")
    rating_df_count.show()
    val rating_df_count_collection = rating_df_count.collect()

    val ds = new org.jfree.data.category.DefaultCategoryDataset
    val mx = scala.collection.immutable.ListMap()

    for( x <- 0 until rating_df_count_collection.length) {
      val occ = rating_df_count_collection(x)(0)
      val count = Integer.parseInt(rating_df_count_collection(x)(1).toString)
      ds.addValue(count,"UserAges", occ.toString)
    }


    //val sorted =  ListMap(ratings_count.toSeq.sortBy(_._1):_*)
    //val ds = new org.jfree.data.category.DefaultCategoryDataset
    //sorted.foreach{ case (k,v) => ds.addValue(v,"Rating Values", k)}

    val chart = ChartFactories.BarChart(ds)
    val font = new Font("Dialog", Font.PLAIN,5);

    chart.peer.getCategoryPlot.getDomainAxis().
      setCategoryLabelPositions(CategoryLabelPositions.UP_90);
    chart.peer.getCategoryPlot.getDomainAxis.setLabelFont(font)
    chart.show()
    Util.sc.stop()
  }
}
