package org.sparksamples.decisiontree

import org.jfree.data.category.DefaultCategoryDataset
import org.jfree.ui.RefineryUtilities
import org.sparksamples.chart.LineChart

/**
  * Decision Tree Max Bins
  * @author Rajdeep Dua
  */
object DecisionTreeMaxBins{

  def main(args: Array[String]) {

    val data = DecisionTreeUtil.getTrainTestData()
    val train_data = data._1
    val test_data = data._2
    val iterations = 10
    val bins_param = Array(2, 4, 8, 16, 32, 64, 100)
    val maxDepth = 5
    val categoricalFeaturesInfo = scala.Predef.Map[Int, Int]()
    val i = 0
    val results = new Array[String](5)
    val resultsMap = new scala.collection.mutable.HashMap[String, String]
    val dataset = new DefaultCategoryDataset()
    for(i <- 0 until bins_param.length) {
      val bin = bins_param(i)
      val rmsle = {

        DecisionTreeUtil.evaluate(train_data, test_data, categoricalFeaturesInfo, 5, bin)
      }

      resultsMap.put(bin.toString,rmsle.toString)
      dataset.addValue(rmsle, "RMSLE", bin)
    }
    val chart = new LineChart(
      "MaxBins" ,
      "DecisionTree : RMSLE vs MaxBins")
    chart.exec("MaxBins","RMSLE",dataset)
    chart.pack( )
    RefineryUtilities.centerFrameOnScreen( chart )
    chart.setVisible( true )
    print(resultsMap)
  }

}
