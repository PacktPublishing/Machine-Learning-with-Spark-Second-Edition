package org.sparksamples.linearregression

import scalax.chart.api._

/**
  * @author Rajdeep Dua
  * Date 5/7/16.
  */

object TestPlot {

  def main(args: Array[String]) {
    val data = for (i <- 1 to 5) yield (i,i)
    val chart = XYLineChart(data)
    chart.show()

  }
}
