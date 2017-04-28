package org.sparksamples

import scala.collection.immutable.ListMap
import scalax.chart.module.ChartFactories

/**
  * Created by Rajdeep Dua on 2/22/16.
  */
object MovieAgesChart {

  def main(args: Array[String]) {
    val movie_data = Util.getMovieData()
    val movie_ages = Util.getMovieAges(movie_data)
    val movie_ages_sorted = ListMap(movie_ages.toSeq.sortBy(_._1):_*)
    val ds = new org.jfree.data.category.DefaultCategoryDataset
    movie_ages_sorted foreach (x => ds.addValue(x._2,"Movies", x._1))
    //0 -> 65, 1 -> 286, 2 -> 355, 3 -> 219, 4 -> 214, 5 -> 126
    val chart = ChartFactories.BarChart(ds)
    chart.show()
    Util.sc.stop()
  }
}
