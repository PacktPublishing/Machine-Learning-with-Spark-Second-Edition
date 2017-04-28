package org.sparksamples

import org.apache.spark.mllib.regression.{LabeledPoint, RidgeRegressionWithSGD}
import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.collection.mutable.ListBuffer

/**
 * A simple Spark app in Scala
 */
object RidgeRegressionApp{

  def get_mapping(rdd :RDD[Array[String]], idx: Int) : Map[String, Long] = {
    return rdd.map( fields=> fields(idx)).distinct().zipWithIndex().collectAsMap()
  }

  def main(args: Array[String]) {
    //val sc = new SparkContext("local[2]", "First Spark App")
    val sc = Util.sc

    // we take the raw data in CSV format and convert it into a set of records
    // of the form (user, product, price)
    val rawData = sc.textFile("../data/hour_noheader.csv")
    val numData = rawData.count()
    val records = rawData.map(line => line.split(","))
    records.cache()
    //print("Mapping of first categorical feature column: " +  get_mapping(records, 2))
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

    print("Feature vector length for categorical features:"+ catLen)
    print("Feature vector length for numerical features:" + numLen)
    print("Total feature vector length: " + totalLen)

    val data = {
      records.map(r => LabeledPoint(Util.extractLabel(r), Util.extractFeatures(r, catLen, mappings)))
    }
    val first_point = data.first()
    println("Linear Model feature vector:" + first_point.features.toString)
    println("Linear Model feature vector length: " + first_point.features.size)

    val iterations = 10
    val step = 0.1
    val intercept =false
    val rr = new RidgeRegressionWithSGD()
    rr.optimizer.setNumIterations(iterations)
    rr.optimizer.setStepSize(0.1)
    val rrModel = rr.run(data)
    val true_vs_predicted = data.map(p => (p.label, rrModel.predict(p.features)))
    val true_vs_predicted_take5 = true_vs_predicted.take(5)
    for(i <- 0 until 5) {
      println("True vs Predicted: " + "i :" + true_vs_predicted_take5(i))
    }
    val mse = true_vs_predicted.map{ case(t, p) => Util.squaredError(t, p)}.mean()
    val mae = true_vs_predicted.map{ case(t, p) => Util.absError(t, p)}.mean()
    val rmsle = Math.sqrt(true_vs_predicted.map{ case(t, p) => Util.squaredLogError(t, p)}.mean())

    println("Ridge Regression - Mean Squared Error: "  + mse)
    println("Ridge Regression  - Mean Absolute Error: " + mae)
    println("Ridge Regression  - Root Mean Squared Log Error:" + rmsle)
  }
}
