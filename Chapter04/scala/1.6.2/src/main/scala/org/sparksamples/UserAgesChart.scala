package org.sparksamples

import scala.collection.immutable.ListMap
import scalax.chart.module.ChartFactories

/**
  * Created by Rajdeep Dua on 2/22/16.
  */
object UserAgesChart {

  def main(args: Array[String]) {
    val user_data = Util.getUserData()
    val user_fields = user_data.map(l => l.split("\\|"))
    val ages = user_fields.map( x => (x(1).toInt)).collect()
    println(ages.getClass.getName)
    val min = 0
    val max = 80
    val bins = 16
    val step = (80/bins).toInt

    var mx = Map(0 -> 0)
    for (i <- step until (max + step) by step) {
      mx += (i -> 0);
    }

    for(i <- 0 until ages.length){
      for (j <- 0 until (max + step) by step) {
        if(ages(i) >= (j) && ages(i) < (j + step)){
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
