package org.sparksamples

import scala.collection.immutable.ListMap
import scalax.chart.module.ChartFactories
import java.awt.Font
import org.jfree.chart.axis.CategoryLabelPositions

/**
  * Created by Rajdeep Dua on 2/22/16.
  */
object CountByRatingChart {

  def main(args: Array[String]) {
    val rating_data_raw = Util.sc.textFile("../../data/ml-100k/u.data")
    val rating_data = rating_data_raw.map(line => line.split("\t"))
    val ratings = rating_data.map(fields => fields(2).toInt)
    val ratings_count = ratings.countByValue()

    val sorted =  ListMap(ratings_count.toSeq.sortBy(_._1):_*)
    val ds = new org.jfree.data.category.DefaultCategoryDataset
    sorted.foreach{ case (k,v) => ds.addValue(v,"Rating Values", k)}

    val chart = ChartFactories.BarChart(ds)
    val font = new Font("Dialog", Font.PLAIN,5);

    chart.peer.getCategoryPlot.getDomainAxis().
      setCategoryLabelPositions(CategoryLabelPositions.UP_90);
    chart.peer.getCategoryPlot.getDomainAxis.setLabelFont(font)
    chart.show()
    Util.sc.stop()
  }
}
