package org.sparksamples.decisiontree

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.rdd.RDD
import org.sparksamples.Util

import scala.collection.Map
import scala.collection.mutable.ListBuffer

/**
 * A simple Spark app in Scala
 */
object DecisionTreeWithLog{

  def get_mapping(rdd :RDD[Array[String]], idx: Int) : Map[String, Long] = {
    return rdd.map( fields=> fields(idx)).distinct().zipWithIndex().collectAsMap()
  }

  def main(args: Array[String]) {
    val save = false
    val sc = Util.sc

    // we take the raw data in CSV format and convert it into a set of records
    // of the form (user, product, price)
    val rawData = sc.textFile("../data/hour_noheader.csv")
    val numData = rawData.count()

    val records = rawData.map(line => line.split(","))
    val first = records.first()

    println(numData.toInt)
    records.cache()
    print("Mapping of first categorical feature column: " +  get_mapping(records, 2))
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

    println("Feature vector length for categorical features:"+ catLen)
    println("Feature vector length for numerical features:" + numLen)
    println("Total feature vector length: " + totalLen)


    val data_dt = {
      records.map(r => LabeledPoint(Math.log(Util.extractLabel(r)), Util.extract_features_dt(r)))
    }
    val first_point = data_dt.first()
    println("Decision Tree feature vector:" + first_point.features.toString)
    println("Decision Tree feature vector length: " + first_point.features.size)

    val categoricalFeaturesInfo = scala.Predef.Map[Int, Int]()
    val impurity = "variance"
    val maxDepth = 5
    val maxBins = 32

    val decisionTreeModel = DecisionTree.trainRegressor(data_dt, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins )

    val preds = decisionTreeModel.predict(data_dt.map( p=> p.features))
    val preds_2 = preds.map(p=> Math.exp(p))
    val actual = data_dt.map( p=> Math.exp(p.label))
    val true_vs_predicted_dt = actual.zip(preds)

    if(save){
      val true_vs_predicted_csv = data_dt.map(p => p.label + " ,"  + decisionTreeModel.predict(p.features))
      val format = new java.text.SimpleDateFormat("dd-MM-yyyy-hh-mm-ss")
      val date = format.format(new java.util.Date())
      true_vs_predicted_csv.saveAsTextFile("./output/decision_tree_" + date + ".csv")
    }

    print("Decision Tree depth: " + decisionTreeModel.depth)
    print("Decision Tree number of nodes: " + decisionTreeModel.numNodes)

    Util.calculatePrintMetrics(true_vs_predicted_dt, "Decision Tree With Log")
    Util.sc.stop()
  }

}
