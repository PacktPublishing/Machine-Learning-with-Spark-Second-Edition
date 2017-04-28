package org.sparksamples

import org.apache.spark.mllib.regression.{IsotonicRegression, LabeledPoint}
import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.collection.mutable.ListBuffer
/**
 * A simple Spark app in Scala
 */
object IsotonicRegressionApp{

  def get_mapping(rdd :RDD[Array[String]], idx: Int) : Map[String, Long] = {
    return rdd.map( fields=> fields(idx)).distinct().zipWithIndex().collectAsMap()
  }

  def main(args: Array[String]) {
    val sc = Util.sc

    // we take the raw data in CSV format and convert it into a set of records
    // of the form (user, product, price)
    val rawData = sc.textFile("../data/hour_noheader_1000.csv")
    val numData = rawData.count()
    val records = rawData.map(line => line.split(","))
    records.cache()

    var list = new ListBuffer[Map[String, Long]]()
    for( i <- 2 to 9){
      val m = get_mapping(records, i)
      list += m
    }
    val mappings = list.toList
    var catLen = 0
    mappings.foreach( m => (catLen +=m.size))

    val numLen = records.first().slice(11, 15).size
    val totalLen = catLen + numLen


    val data = {
      records.map(r => LabeledPoint(Util.extractLabel(r), Util.extractFeatures(r, catLen, mappings)))
    }
    val parsedData = records.map { r =>
      (Util.extractLabel(r), Util.extractSumFeature(r, catLen, mappings), 1.0)
    }

    val iterations = 10
    val step = 0.1
    val intercept =false

    val x = new IsotonicRegression().setIsotonic(false)
    val model = x.run(parsedData)

    val parsedData1: RDD[Double] = parsedData.map(r => r._2)
    //val model = GradientBoostedTrees.train(data, boostingStrategy)
    val true_vs_predicted = parsedData.map(p => (p._1, model.predict(p._2)))

    val save = true
    if(save){
      val true_vs_predicted_csv = parsedData.map(p => ( p._1+ "," + model.predict(p._2)))
      val format = new java.text.SimpleDateFormat("dd-MM-yyyy-hh-mm-ss")
      val date = format.format(new java.util.Date())
      true_vs_predicted_csv.saveAsTextFile("./output/isotonic_regression_" + date + ".csv")
    }
    val true_vs_predicted_take5 = true_vs_predicted.take(5)
    for(i <- 0 until 5) {
      println("True vs Predicted: " + "i :" + true_vs_predicted_take5(i))
    }

    val mse = true_vs_predicted.map{ case(t, p) => Util.squaredError(t, p)}.mean()
    val mae = true_vs_predicted.map{ case(t, p) => Util.absError(t, p)}.mean()
    val rmsle = Math.sqrt(true_vs_predicted.map{ case(t, p) => Util.squaredLogError(t, p)}.mean())

    Util.calculatePrintMetrics(true_vs_predicted, "Isotonic Regression")

  }
}
