package org.sparksamples.decisiontree

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.rdd.RDD
import org.sparksamples.Util

import scala.collection.Map
import scala.collection.mutable.ListBuffer

/**
  * Created by ubuntu on 5/15/16.
  */
object DecisionTreeUtil {

  def getTrainTestData(): (RDD[LabeledPoint], RDD[LabeledPoint]) = {
    val recordsArray = Util.getRecords()
    val records = recordsArray._1
    val first = records.first()
    val numData = recordsArray._2

    println(numData.toString())
    records.cache()
    print("Mapping of first categorical feature column: " +  Util.get_mapping(records, 2))
    var list = new ListBuffer[Map[String, Long]]()
    for( i <- 2 to 9){
      val m = Util.get_mapping(records, i)
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
    val data_dt = {
      records.map(r => LabeledPoint(Util.extractLabel(r), Util.extract_features_dt(r)))
    }

    val splits = data_dt.randomSplit(Array(0.8, 0.2), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)
    return (training, test)
  }

  def evaluate(train: RDD[LabeledPoint],test: RDD[LabeledPoint],
               categoricalFeaturesInfo: scala.Predef.Map[Int, Int],
                maxDepth :Int, maxBins: Int): Double = {
    val impurity = "variance"
    val decisionTreeModel = DecisionTree.trainRegressor(train, categoricalFeaturesInfo,
      impurity,maxDepth, maxBins )

    val true_vs_predicted = test.map(p => (p.label, decisionTreeModel.predict(p.features)))
    val rmsle = Math.sqrt(true_vs_predicted.map{ case(t, p) => Util.squaredLogError(t, p)}.mean())
    return rmsle
  }

}
