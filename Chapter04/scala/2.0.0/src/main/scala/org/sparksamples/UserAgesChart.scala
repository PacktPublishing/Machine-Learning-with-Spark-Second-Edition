package org.sparksamples

import scala.collection.immutable.ListMap
import scalax.chart.module.ChartFactories

/**
  * Created by Rajdeep Dua on 2/22/16.
  * Modified for DataFrames on 9/3/2016
  */
object UserAgesChart {

  def main(args: Array[String]) {

    val userDataFrame = Util.getUserFieldDataFrame()
    val ages_array = userDataFrame.select("age").collect()

    val min = 0
    val max = 80
    val bins = 16
    val step = (80/bins).toInt
    var mx = Map(0 -> 0)
    for (i <- step until (max + step) by step) {
      mx += (i -> 0)
    }
    for( x <- 0 until ages_array.length) {
      val age = Integer.parseInt(ages_array(x)(0).toString)
      for (j <- 0 until (max + step) by step) {
        if(age >= j && age < (j + step)){
          mx = mx + (j -> (mx(j) + 1))
        }
      }
    }

    val mx_sorted =  ListMap(mx.toSeq.sortBy(_._1):_*)
    val ds = new org.jfree.data.category.DefaultCategoryDataset
    mx_sorted.foreach{ case (k,v) => ds.addValue(v,"UserAges", k)}

    val chart = ChartFactories.BarChart(ds)

    chart.show()
    Util.sc.stop()
  }
}
