package org.sparksamples.gradientboosted

import org.jfree.data.category.DefaultCategoryDataset
import org.jfree.ui.RefineryUtilities
import org.sparksamples.chart.LineChart

/**
  * LogisticalRegression App
  * @author Rajdeep Dua
  */
object GradientBoostedTreesMaxDepth{

  
  def main(args: Array[String]) {

    val data = GradientBoostedTreesUtil.getTrainTestData()
    val train_data = data._1
    val test_data = data._2

    val iterations_param = Array(1, 5, 10, 15, 18)
    val iteration = 5
    val maxDepth_param = Array(1,5, 7, 10)
    val maxBin = 10

    val i = 0
    val resultsMap = new scala.collection.mutable.HashMap[String, String]
    val dataset = new DefaultCategoryDataset()
    for(i <- 0 until maxDepth_param.length) {
      val maxDepth = maxDepth_param(i)
      val rmsle = GradientBoostedTreesUtil.evaluate(train_data, test_data,iteration,maxDepth, maxBin)
      resultsMap.put(maxDepth.toString,rmsle.toString)
      dataset.addValue(rmsle, "RMSLE", maxDepth)
    }
    val chart = new LineChart(
      "MaxDepth" ,
      "GradientBoostedTrees : RMSLE vs MaxDepth")
    chart.exec("MaxDepth","RMSLE",dataset)
    chart.pack( )
    chart.lineChart.getCategoryPlot().getRangeAxis().setRange(1.32, 1.5)
    RefineryUtilities.centerFrameOnScreen( chart )
    chart.setVisible(true)
    print(resultsMap)

  }

}
