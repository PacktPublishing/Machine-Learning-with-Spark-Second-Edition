package org.sparksamples.decisiontree

import org.jfree.data.category.DefaultCategoryDataset
import org.jfree.ui.RefineryUtilities
import org.sparksamples.chart.LineChart

/**
  * LogisticalRegression App
  * @author Rajdeep Dua
  */
object DecisionTreeMaxDepth{

  
  def main(args: Array[String]) {

    val data = DecisionTreeUtil.getTrainTestData()
    val train_data = data._1
    val test_data = data._2
    val iterations = 10
    // params = [2, 4, 8, 16, 32, 64, 100]
    //val steps_param = Array(0.01, 0.025, 0.05, 0.1, 1.0)
    //DecisionTreeMaxBins$ params = [1, 2, 3, 4, 5, 10, 20]
    val bins_param = Array(2, 4, 8, 16, 32, 64, 100)
    val depth_param = Array(1, 2, 3, 4, 5, 10, 20)
    //val maxDepth = 5
    val bin = 32
    val categoricalFeaturesInfo = scala.Predef.Map[Int, Int]()
    val i = 0
    val results = new Array[String](7)
    val resultsMap = new scala.collection.mutable.HashMap[String, String]
    val dataset = new DefaultCategoryDataset()
    for(i <- 0 until depth_param.length) {
      val depth = depth_param(i)
      val rmsle = DecisionTreeUtil.evaluate(train_data, test_data, categoricalFeaturesInfo, depth, bin)

      resultsMap.put(depth.toString,rmsle.toString)
      dataset.addValue(rmsle, "RMSLE", depth)
    }
    val chart = new LineChart(
      "MaxDepth" ,
      "DecisionTree : RMSLE vs MaxDepth")
    chart.exec("MaxDepth","RMSLE",dataset)
    chart.pack()
    RefineryUtilities.centerFrameOnScreen( chart )
    chart.setVisible( true )
    print(resultsMap)

  }

}
