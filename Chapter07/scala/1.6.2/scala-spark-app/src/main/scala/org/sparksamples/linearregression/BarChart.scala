package org.sparksamples.linearregression

import java.awt.{GradientPaint, Color}

import org.jfree.chart.plot.PlotOrientation
import org.jfree.chart.{ChartFactory, ChartPanel}
import org.jfree.data.category.{CategoryDataset, DefaultCategoryDataset}
import org.jfree.ui.{ApplicationFrame, RefineryUtilities}

/**
  * @author Rajdeep Dua
  * Date 5/7/16.
  * reference http://www.tutorialspoint.com/jfreechart/jfreechart_bar_chart.htm
  */



class BarChart(applicationTitle: String, chartTitle: String) extends ApplicationFrame(applicationTitle) {
  def exec(resultsMap: scala.collection.mutable.HashMap[String,String]) =  {
    val results : Array[Double] = Array(0.0, 0.0)
    results(0) = 1.4
    results(1) = 1.5
    val barChart = ChartFactory.createBarChart(
      chartTitle,
      "Category",
      "Score",
      createDataset(resultsMap),
      PlotOrientation.VERTICAL,
      true, true, false);

    var renderer = barChart.getCategoryPlot.getRenderer
    val customColor = new Color(153,76,0);
    var gp0 = new GradientPaint(
      0.0f, 0.0f, Color.DARK_GRAY,
      0.0f, 0.0f, Color.DARK_GRAY
    )
    val gp1 = new GradientPaint(
      0.0f, 0.0f, customColor,
      0.0f, 0.0f, customColor
    )
    val gp2 = new GradientPaint(
      0.0f, 0.0f, Color.red,
      0.0f, 0.0f, Color.red
    )
    renderer.setSeriesPaint(0, gp0)
    renderer.setSeriesPaint(1, customColor)
    //renderer.setSeriesPaint(2, gp2)
    val chartPanel = new ChartPanel(barChart);
    chartPanel.setPreferredSize(new java.awt.Dimension(560, 367));
    setContentPane(chartPanel);
  }

  def createDataset (resultsMap: scala.collection.mutable.HashMap[String,String]) : CategoryDataset ={

    val intercept_true = "true"
    val intercept_false = "false"
    val speed = "Speed"
    val rmsle = "RMSLE"

    val dataset = new DefaultCategoryDataset()
    dataset.addValue(resultsMap(intercept_true).toDouble, intercept_true, rmsle);
    dataset.addValue(resultsMap(intercept_true).toDouble, intercept_false, rmsle);

    return dataset;
  }
}
object BarChart {

  def main(args: Array[String]) {
    val chart = new BarChart("LinearRegressionWith SGD ", "Intercept")
    val resultsMap = new scala.collection.mutable.HashMap[String, String]
    resultsMap.put("true", "1.55")
    resultsMap.put("false", "1.56")
    chart.exec(resultsMap)
    chart.pack()
    RefineryUtilities.centerFrameOnScreen( chart )
    chart.setVisible( true )
  }
}
