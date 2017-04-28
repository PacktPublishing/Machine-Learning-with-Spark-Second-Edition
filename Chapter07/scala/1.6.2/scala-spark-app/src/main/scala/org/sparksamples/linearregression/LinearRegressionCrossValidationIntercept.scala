package org.sparksamples.linearregression

import org.jfree.data.category.DefaultCategoryDataset
import org.jfree.ui.RefineryUtilities
import org.sparksamples.chart.LineChart

/**
  * LogisticalRegression App
  * @author Rajdeep Dua
  */
object LinearRegressionCrossValidationIntercept{
  def main(args: Array[String]) {
    val data = LinearRegressionUtil.getTrainTestData()
    val train_data = data._1
    val test_data = data._2

    val iterations = 10
    val step = 0.1
    val paramsArray = new Array[Boolean](2)
    paramsArray(0) = false
    paramsArray(1) = true
    val i = 0
    val results = new Array[String](2)
    val resultsMap = new scala.collection.mutable.HashMap[String, String]
    val dataset = new DefaultCategoryDataset()
    for(i <- 0 until 2) {
      val intercept = paramsArray(i)
      val rmsle = LinearRegressionUtil.evaluate(train_data, test_data,iterations,step,intercept)
      results(i) = intercept + ":" + rmsle
      resultsMap.put(intercept.toString,rmsle.toString)
      dataset.addValue(rmsle, "RMSLE", intercept.toString)
    }

    val chart = new LineChart(
      "Intercept" ,
      "LinearRegressionWithSGD : RMSLE vs Intercept")
    chart.exec("Steps","RMSLE",dataset)
    chart.lineChart.getCategoryPlot().getRangeAxis().setRange(1.56, 1.58)

    chart.pack( )

    RefineryUtilities.centerFrameOnScreen( chart )
    chart.setVisible( true )

    println(results)

  }

}
