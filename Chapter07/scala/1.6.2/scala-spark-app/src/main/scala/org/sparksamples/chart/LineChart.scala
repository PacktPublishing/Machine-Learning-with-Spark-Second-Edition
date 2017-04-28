package org.sparksamples.chart

import org.jfree.chart.plot.{CategoryPlot, PlotOrientation}
import org.jfree.chart.{ChartFactory, ChartPanel, JFreeChart}
import org.jfree.data.category.{CategoryDataset, DefaultCategoryDataset}
import org.jfree.ui.{ApplicationFrame, RefineryUtilities}

class LineChart(applicationTitle: String, chartTitle: String) extends ApplicationFrame(applicationTitle) {
  //categoryAxisLabel - the label for the category axis (null permitted).
  //valueAxisLabel
  var lineChart:JFreeChart = null
  def exec(categoryAxisLabel : String, valueAxisLabel :String, dataset: CategoryDataset) = {
    lineChart = ChartFactory.createLineChart(
      chartTitle,
      categoryAxisLabel,
      valueAxisLabel,
      dataset,
      PlotOrientation.VERTICAL,
      true, true, false)


    val chartPanel = new ChartPanel(lineChart)
    chartPanel.setPreferredSize(new java.awt.Dimension(560, 367))
    setContentPane(chartPanel)
  }

  def getCategoryPlot(): CategoryPlot = {
    return lineChart.getCategoryPlot()

  }

}

object LineChart {
   def main(args: Array[String]) {
     val dataset = createDataset()
    val chart = new LineChart(
      "School Vs Years" ,
      "Numer of Schools vs years")
     chart.exec("Years","No of Schools",dataset)
    chart.pack( )
    RefineryUtilities.centerFrameOnScreen( chart )
    chart.setVisible( true )
  }

  def createDataset(): CategoryDataset = {
    val dataset = new DefaultCategoryDataset()
    dataset.addValue(15, "schools", "1970")
    dataset.addValue(30, "schools", "1980")
    dataset.addValue(60, "schools", "1990")
    dataset.addValue(120, "schools", "2000")
    dataset.addValue(240, "colleges", "2010")
    dataset.addValue(300, "colleges", "2014")
    return dataset
  }
}