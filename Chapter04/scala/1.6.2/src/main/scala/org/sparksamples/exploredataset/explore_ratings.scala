package org.sparksamples.exploredataset

import java.io.File

import breeze.linalg.{DenseVector, norm}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.linalg.Vectors
import org.jfree.chart.plot.PlotOrientation
import org.jfree.chart.{ChartFactory, ChartUtilities}
import org.jfree.data.category.DefaultCategoryDataset
import org.jfree.data.statistics.{HistogramDataset, HistogramType}

/**
  * Created by manpreet.singh on 27/02/16.
  */
object explore_ratings {

  def main(args: Array[String]) {
    val sc = new SparkContext("local[2]", "Explore Users in Movie Dataset")

    val rating_fields = sc.textFile("/Users/manpreet.singh/Sandbox/codehub/github/machinelearning/breeze.io/src/main/scala/moviestream/ml-100k/u.data").map(line => line.split("\t"))
      .map(records => (records(0), records(1), records(2), records(3)))

    val num_ratings = rating_fields.count()
    println(num_ratings)

    val ratings = rating_fields.map(rating_fields => rating_fields._3.toDouble)
    val min_rating = ratings.min()
    val max_rating = ratings.max()
    val mean_rating = ratings.mean()

    val median = ratings.collect()
    val med = median.sortWith(_ < _).drop(median.length/2).head

    val ratings_per_user = ratings.count()/943
    val ratings_per_movie = ratings.count()/1682

    println(min_rating)
    println(max_rating)
    println(mean_rating)
    println(med)
    println(ratings_per_user)
    println(ratings_per_movie)
    println(ratings.stats())

    val rats = ratings.map(ratings => (ratings,1)).reduceByKey(_+_).collect()
    val sortedRatings = rats.sortWith(_._1 < _._1)
    val dataset2 = new DefaultCategoryDataset()
    sortedRatings.foreach(println)
    for (rat <- sortedRatings) {
      dataset2.setValue(rat._2, "count", rat._1);
    }

    val plotTitle2 = "Ratings Chart";
    val xaxis2 = "Ratings";
    val yaxis2 = "count";
    val orientation2 = PlotOrientation.VERTICAL;
    val show2 = false;
    val toolTips2 = false;
    val urls2 = false;
    val chart2 = ChartFactory.createBarChart( plotTitle2, xaxis2, yaxis2, dataset2, orientation2, show2, toolTips2, urls2);
    val width2 = 900;
    val height2 = 500;
    ChartUtilities.saveChartAsPNG(new File("/Users/manpreet.singh/Sandbox/codehub/github/machinelearning/breeze.io/src/main/scala/moviestream/plots/ratings_chart.png"), chart2, width2, height2);

    val rating_user = rating_fields.map(rating_fields => (rating_fields._1.toInt,rating_fields._3.toInt)).groupByKey()
    val user_ratings_byuser = rating_user.map(rating_user => (rating_user._1, rating_user._2.size))
    user_ratings_byuser.sortByKey().take(5).foreach(println)

    val dataset1 = new HistogramDataset()
    dataset1.setType(HistogramType.RELATIVE_FREQUENCY)
    val user_ratings_ByUser = user_ratings_byuser.map(user_ratings_byuser => user_ratings_byuser._2.toDouble).collect()
    dataset1.addSeries("Histogram", user_ratings_ByUser, 200)
    val plotTitle1 = "Age Histogram";
    val xaxis1 = "age";
    val yaxis1 = "scale";
    val orientation1 = PlotOrientation.VERTICAL;
    val show1 = false;
    val toolTips1 = false;
    val urls1 = false;
    val chart1 = ChartFactory.createHistogram( plotTitle1, xaxis1, yaxis1, dataset1, orientation1, show1, toolTips1, urls1);
    val width1 = 2000;
    val height1 = 400;
    ChartUtilities.saveChartAsPNG(new File("/Users/manpreet.singh/Sandbox/codehub/github/machinelearning/breeze.io/src/main/scala/moviestream/plots/user_ratings_byuser_histogram.png"), chart1, width1, height1);


    //val vector = DenseVector.rand(10)
    val vector = DenseVector(0.49671415, -0.1382643, 0.64768854, 1.52302986, -0.23415337, -0.23413696, 1.57921282, 0.76743473, -0.46947439, 0.54256004)
    val temp = norm(vector)
    println(temp)
    val vec = vector/temp
    println(vec)

    val v = Vectors.dense(0.49671415, -0.1382643, 0.64768854, 1.52302986, -0.23415337, -0.23413696, 1.57921282, 0.76743473, -0.46947439, 0.54256004)
    val normalizer = new Normalizer(2)
    val norm_op = normalizer.transform(v)
    println(norm_op)
    sc.stop()
  }

  def convert(year:String): String = {
    try{
      val mod_year = year.substring(year.length - 4,year.length)
      return mod_year
    }catch {
      case e : Exception => return "1900"
    }

  }

}
