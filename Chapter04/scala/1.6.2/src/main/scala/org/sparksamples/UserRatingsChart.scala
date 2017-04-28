package org.sparksamples

import scala.collection.immutable.ListMap
import scalax.chart.module.ChartFactories

/**
  * Created by Rajdeep Dua on 2/22/16.
  */
object UserRatingsChart {

  def main(args: Array[String]) {
    val user_data = Util.getUserData()
    val user_fields = user_data.map(l => l.split("\\|"))
    val ages = user_fields.map( x => (x(1).toInt)).collect()

    val rating_data_raw = Util.sc.textFile("../../data/ml-100k/u.data")
    val rating_data = rating_data_raw.map(line => line.split("\t"))
    val user_ratings_grouped = rating_data.map(
      fields => (fields(0).toInt, fields(2).toInt)).groupByKey()
    val user_ratings_byuser = user_ratings_grouped.map(v =>  (v._1,v._2.size))
    val user_ratings_byuser_local = user_ratings_byuser.map(v =>  v._2).collect()
    val input = user_ratings_byuser_local
    val min = 0
    val max = 500
    val bins = 200
    val step = (max/bins).toInt

    var mx = Map(0 -> 0)
    for (i <- step until (max + step) by step) {
      mx += (i -> 0);
    }

    for(i <- 0 until input.length){
      for (j <- 0 until (max + step) by step) {
        if(ages(i) >= (j) && input(i) < (j + step)){
          mx = mx + (j -> (mx(j) + 1))
        }
      }
    }

    val mx_sorted =  ListMap(mx.toSeq.sortBy(_._1):_*)
    val ds = new org.jfree.data.category.DefaultCategoryDataset
    mx_sorted.foreach{ case (k,v) => ds.addValue(v,"Ratings", k)}

    val chart = ChartFactories.BarChart(ds)

    chart.show()
    Util.sc.stop()
  }
}
