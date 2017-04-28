package org.sparksamples.gradientboosted

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.rdd.RDD
import org.sparksamples.Util

import scala.collection.Map
import scala.collection.mutable.ListBuffer

/**
  * Created by ubuntu on 5/15/16.
  */
object GradientBoostedTreesUtil {

  def getTrainTestData(): (RDD[LabeledPoint], RDD[LabeledPoint]) = {
    val recordsArray = Util.getRecords()
    val records = recordsArray._1
    val first = records.first()
    val numData = recordsArray._2

    println(numData.toString())
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

    val data = {
      records.map(r => LabeledPoint(Util.extractLabel(r), Util.extractFeatures(r, catLen, mappings)))
    }


    val splits = data.randomSplit(Array(0.8, 0.2), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)
    return (training, test)
  }

  def get_mapping(rdd :RDD[Array[String]], idx: Int) : Map[String, Long] = {
    return rdd.map( fields=> fields(idx)).distinct().zipWithIndex().collectAsMap()
  }



  def evaluate(train: RDD[LabeledPoint],test: RDD[LabeledPoint], iterations:Int, maxDepth:Int,
               maxBins: Int): Double ={

    var boostingStrategy = BoostingStrategy.defaultParams("Regression")
    boostingStrategy.setNumIterations(iterations)
    boostingStrategy.treeStrategy.setMaxDepth(maxDepth)
    boostingStrategy.treeStrategy.setMaxBins(maxBins)

    val model = GradientBoostedTrees.train(train, boostingStrategy)
//
//    @classmethod
//    @since("1.3.0")
//    def trainRegressor(cls, data, categoricalFeaturesInfo,
//                       loss="leastSquaresError", numIterations=100, learningRate=0.1, maxDepth=3,
//                       maxBins=32):

    val true_vs_predicted = test.map(p => (p.label, model.predict(p.features)))
    val rmsle = Math.sqrt(true_vs_predicted.map{ case(t, p) => Util.squaredLogError(t, p)}.mean())
    return rmsle
  }

}
