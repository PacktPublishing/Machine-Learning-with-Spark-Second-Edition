package org.sparksamples.exploredataset

import breeze.linalg.CSCMatrix
import org.apache.spark.mllib.feature.Word2Vec
import org.jfree.chart.ChartFactory
import org.jfree.chart.plot.PlotOrientation
import org.jfree.data.statistics.{HistogramDataset, HistogramType}
import org.sparksamples.Util

import scala.collection.mutable.ListBuffer

/**
  * Created by manpreet.singh on 27/02/16.
  */
object explore_movies {

  def main(args: Array[String]) {
    val sc = Util.sc

    val movie_fields = sc.textFile(Util.MOVIE_DATA).map(line => line.split("\\|"))
      .map(records => (records(0), records(1), records(2), records(3), records(4)))

    val num_movies = movie_fields.count()
    println(num_movies)

    val years = movie_fields.map(movie_fields => convert(movie_fields._3)).collect()
    val years_filtered = years.filter(years => years != "1900")
    val years_filtered_int = years_filtered.map(years_filtered => years_filtered.toInt)

    val movie_ages = years_filtered_int.map(years_filtered_int => 1998 - years_filtered_int)
    movie_ages.foreach(println)

    val dataset1 = new HistogramDataset()
    dataset1.setType(HistogramType.RELATIVE_FREQUENCY)
    val movie_age_series = movie_ages.map(movie_ages => movie_ages.toDouble)
    dataset1.addSeries("Histogram", movie_age_series, 10)
    val plotTitle1 = "Age Histogram";
    val xaxis1 = "age";
    val yaxis1 = "scale";
    val orientation1 = PlotOrientation.VERTICAL;
    val show1 = false;
    val toolTips1 = false;
    val urls1 = false;
    val chart1 = ChartFactory.createHistogram( plotTitle1, xaxis1, yaxis1, dataset1, orientation1, show1, toolTips1, urls1);
    val width1 = 600;
    val height1 = 400;
    //ChartUtilities.saveChartAsPNG(new File("/Users/manpreet.singh/Sandbox/codehub/github/machinelearning/breeze.io/src/main/scala/moviestream/plots/movieage_histogram.png"), chart1, width1, height1);

    val raw_title = movie_fields.map(movie_fields => movie_fields._2)
    val pattern = "^[^(]*".r
    val proc_title = raw_title.map(raw_title => pattern.findFirstIn(raw_title))
    val title = proc_title.map(proc_title => proc_title.get.trim)
    title.take(5).foreach(println)

    val title_terms = title.map(title => title.split(" "))
    title_terms.take(5).foreach(_.foreach(println))
    println(title_terms.count())

    val all_terms_dic = new ListBuffer[String]()
    val all_terms = title_terms.flatMap(title_terms => title_terms).distinct().collect()
    for (term <- all_terms){
      all_terms_dic += term
    }

    println(all_terms_dic.length)
    println(all_terms_dic.indexOf("Dead"))
    println(all_terms_dic.indexOf("Rooms"))

    val all_terms_withZip = title_terms.flatMap(title_terms => title_terms).distinct().zipWithIndex().collectAsMap()
    println(all_terms_withZip.get("Dead"))
    println(all_terms_withZip.get("Rooms"))

    val word2vec = new Word2Vec()
    val rdd_terms = title.map(title => title.split(" ").toSeq)
    val model = word2vec.fit(rdd_terms)
    println(model.findSynonyms("Dead", 40))

    val term_vectors = title_terms.map(title_terms => create_vector(title_terms, all_terms_dic))
    term_vectors.take(5).foreach(println)

    sc.stop()
  }

  def create_vector(title_terms:Array[String], all_terms_dic:ListBuffer[String]): CSCMatrix[Int] = {
    var idx = 0
    val x = CSCMatrix.zeros[Int](1, all_terms_dic.length)
    title_terms.foreach(i => {
      if (all_terms_dic.contains(i)) {
        idx = all_terms_dic.indexOf(i)
        x.update(0, idx, 1)
      }
    })
    return x
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
