package org.sparksamples.gradientboosted

import org.jfree.data.category.DefaultCategoryDataset
import org.jfree.ui.RefineryUtilities
import org.sparksamples.chart.LineChart

/**
  * @author Rajdeep Dua
  */
object GradientBoostedTreesIterations{

  
  def main(args: Array[String]) {

    val data = GradientBoostedTreesUtil.getTrainTestData()
    val train_data = data._1
    val test_data = data._2

    val iterations_param = Array(1, 5, 10, 15, 18)
    val maxDepth =5
    val maxBins = 16

    val i = 0
    val resultsMap = new scala.collection.mutable.HashMap[String, String]
    val dataset = new DefaultCategoryDataset()
    for(i <- 0 until iterations_param.length) {
      val iteration = iterations_param(i)
      val rmsle = GradientBoostedTreesUtil.evaluate(train_data, test_data,iteration,maxDepth, maxBins)
      resultsMap.put(iteration.toString,rmsle.toString)
      dataset.addValue(rmsle, "RMSLE", iteration)
    }
    val chart = new LineChart(
      "Iterations" ,
      "GradientBoostedTrees : RMSLE vs Iterations")
    chart.exec("Iterations","RMSLE",dataset)
    chart.pack( )
    chart.lineChart.getCategoryPlot().getRangeAxis().setRange(1.32, 1.37)
    RefineryUtilities.centerFrameOnScreen( chart )
    chart.setVisible( true )
    print(resultsMap)

  }

}
