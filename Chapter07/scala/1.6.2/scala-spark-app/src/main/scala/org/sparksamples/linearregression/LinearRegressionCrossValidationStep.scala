package org.sparksamples.linearregression

import org.jfree.data.category.DefaultCategoryDataset
import org.jfree.ui.RefineryUtilities
import org.sparksamples.chart.LineChart

/**
  * LogisticalRegression App
  * @author Rajdeep Dua
  */
object LinearRegressionCrossValidationStep{
  def main(args: Array[String]) {
    val data = LinearRegressionUtil.getTrainTestData()
    val train_data = data._1
    val test_data = data._2
    val iterations = 10
    val steps_param = Array(0.01, 0.025, 0.05, 0.1, 1.0)
    val intercept =false

    val i = 0
    val results = new Array[String](5)
    val resultsMap = new scala.collection.mutable.HashMap[String, String]
    val dataset = new DefaultCategoryDataset()
    for(i <- 0 until steps_param.length) {
      val step = steps_param(i)
      val rmsle = LinearRegressionUtil.evaluate(train_data, test_data,iterations,step,intercept)
      //results(i) = step + ":" + rmsle
      resultsMap.put(step.toString,rmsle.toString)
      dataset.addValue(rmsle, "RMSLE", Math.log(step))
    }
    val chart = new LineChart(
      "Steps" ,
      "LinearRegressionWithSGD : RMSLE vs Steps")
    chart.exec("Steps - Log Scale","RMSLE",dataset)
    chart.pack( )
    RefineryUtilities.centerFrameOnScreen( chart )
    chart.setVisible( true )
    print(resultsMap)

  }

}
