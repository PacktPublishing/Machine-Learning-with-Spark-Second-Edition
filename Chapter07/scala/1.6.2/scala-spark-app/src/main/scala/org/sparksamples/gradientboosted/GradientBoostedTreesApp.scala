package org.sparksamples.gradientboosted

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.rdd.RDD
import org.sparksamples.Util

import scala.collection.Map
import scala.collection.mutable.ListBuffer

/**
 * @author Rajdeep Dua
 */
object GradientBoostedTreesApp{

  def get_mapping(rdd :RDD[Array[String]], idx: Int) : Map[String, Long] = {
    return rdd.map( fields=> fields(idx)).distinct().zipWithIndex().collectAsMap()
  }

  def main(args: Array[String]) {
    //val conf = new SparkConf().setMaster("local").setAppName("GradientBoostedTreesRegressionApp")
    val sc = Util.sc

    // we take the raw data in CSV format and convert it into a set of records
    // of the form (user, product, price)
    val rawData = sc.textFile("../data/hour_noheader.csv")
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

    print("Feature vector length for categorical features:"+ catLen)
    print("Feature vector length for numerical features:" + numLen)
    print("Total feature vector length: " + totalLen)

    val data = {
      records.map(r => LabeledPoint(Util.extractLabel(r), Util.extractFeatures(r, catLen, mappings)))
    }
    val first_point = data.first()
    println("Gradient Boosted Trees Model feature vector:" + first_point.features.toString)
    println("Gradient Boosted Trees Model feature vector length: " + first_point.features.size)


    var boostingStrategy = BoostingStrategy.defaultParams("Regression")
    boostingStrategy.setNumIterations(3)// Note: Use more iterations in practice.
    boostingStrategy.treeStrategy.setMaxDepth(5)


    val model = GradientBoostedTrees.train(data, boostingStrategy)
    val true_vs_predicted = data.map(p => (p.label, model.predict(p.features)))
    val true_vs_predicted_take5 = true_vs_predicted.take(5)
    for(i <- 0 until 5) {
      println("True vs Predicted: " + "i :" + true_vs_predicted_take5(i))
    }
    val save = true
    if(save){
      val true_vs_predicted_csv = data.map(p => p.label + " ,"  + model.predict(p.features))
      val format = new java.text.SimpleDateFormat("dd-MM-yyyy-hh-mm-ss")
      val date = format.format(new java.util.Date())
      true_vs_predicted_csv.saveAsTextFile("./output/gradient_boosted_trees_" + date + ".csv")
    }
    val mse = true_vs_predicted.map{ case(t, p) => Util.squaredError(t, p)}.mean()
    val mae = true_vs_predicted.map{ case(t, p) => Util.absError(t, p)}.mean()
    val rmsle = Math.sqrt(true_vs_predicted.map{ case(t, p) => Util.squaredLogError(t, p)}.mean())

    println("Gradient Boosted Trees - Mean Squared Error: "  + mse)
    println("Gradient Boosted Trees - Mean Absolute Error: " + mae)
    println("Gradient Boosted Trees - Root Mean Squared Log Error:" + rmsle)
  }
}
