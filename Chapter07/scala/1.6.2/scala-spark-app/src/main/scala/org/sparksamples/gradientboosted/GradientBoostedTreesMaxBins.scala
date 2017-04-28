package org.sparksamples.gradientboosted

import org.jfree.data.category.DefaultCategoryDataset
import org.jfree.ui.RefineryUtilities
import org.sparksamples.chart.LineChart

/**
  * @author Rajdeep Dua
  */
object GradientBoostedTreesMaxBins{

  
  def main(args: Array[String]) {

    val data = GradientBoostedTreesUtil.getTrainTestData()
    val train_data = data._1
    val test_data = data._2
//    params = [10, 16, 32, 64]
//    lrRate = 0.1
//    mxDepth = 3
//    nmIterations = 10
    val maxBins_param = Array(10,16,32,64)
    val iteration = 10
    val maxDepth = 3

    val i = 0
    val resultsMap = new scala.collection.mutable.HashMap[String, String]
    val dataset = new DefaultCategoryDataset()
    for(i <- 0 until maxBins_param.length) {
      val maxBin = maxBins_param(i)
      val rmsle = GradientBoostedTreesUtil.evaluate(train_data, test_data,iteration,maxDepth,maxBin)
      // def evaluate(train: RDD[LabeledPoint],test: RDD[LabeledPoint], iterations:Int, maxDepth:Int,
      // maxBins: Int)
      resultsMap.put(maxBin.toString,rmsle.toString)
      dataset.addValue(rmsle, "RMSLE", maxBin)
    }
    val chart = new LineChart(
      "Max Bin" ,
      "GradientBoostedTrees : RMSLE vs MaxBin")
    chart.exec("MaxBins","RMSLE",dataset)
    chart.pack( )
    chart.lineChart.getCategoryPlot().getRangeAxis().setRange(1.35, 1.37)
    RefineryUtilities.centerFrameOnScreen( chart )
    chart.setVisible(true)
    print(resultsMap)

  }

}
