package org.sparksamples

import scala.collection.immutable.ListMap
import scalax.chart.module.ChartFactories
import java.awt.Font
import org.jfree.chart.axis.CategoryLabelPositions

/**
  * Created by Rajdeep Dua on 2/22/16.
  */
object UserOccupationChart {

  def main(args: Array[String]) {
    val user_data = Util.getUserData()
    val user_fields = user_data.map(l => l.split("\\|"))
    val count_by_occupation = user_fields.map( fields => (fields(3), 1)).
      reduceByKey( (x, y) => x + y).collect()
    println(count_by_occupation)

    val sorted =  ListMap(count_by_occupation.toSeq.sortBy(_._2):_*)
    val ds = new org.jfree.data.category.DefaultCategoryDataset
    sorted.foreach{ case (k,v) => ds.addValue(v,"UserAges", k)}

    val chart = ChartFactories.BarChart(ds)
    val font = new Font("Dialog", Font.PLAIN,5);

    chart.peer.getCategoryPlot.getDomainAxis().
      setCategoryLabelPositions(CategoryLabelPositions.UP_90);
    chart.peer.getCategoryPlot.getDomainAxis.setLabelFont(font)
    chart.show()
    Util.sc.stop()
  }
}
