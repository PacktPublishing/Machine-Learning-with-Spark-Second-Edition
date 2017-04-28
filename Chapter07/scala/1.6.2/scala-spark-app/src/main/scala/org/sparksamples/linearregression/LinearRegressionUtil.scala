package org.sparksamples.linearregression

import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.rdd.RDD
import org.sparksamples.Util

import scala.collection.Map
import scala.collection.mutable.ListBuffer
/**
  * Created by ubuntu on 5/15/16.
  */
object LinearRegressionUtil {

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

    val splits = data.randomSplit(Array(0.8, 0.2), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)
    return (training, test)
  }


  /*def evaluate(train, test, iterations, step, regParam, regType, intercept):
  model = LinearRegressionWithSGD.train(train, iterations, step, regParam=regParam, regType=regType, intercept=intercept)
  tp = test.map(lambda p: (p.label, model.predict(p.features)))
  rmsle = np.sqrt(tp.map(lambda (t, p): squared_log_error(t, p)).mean())
  return rmsle*/


  def evaluate(train: RDD[LabeledPoint],test: RDD[LabeledPoint], iterations:Int,step:Double,
               intercept:Boolean): Double ={
    val linReg = new LinearRegressionWithSGD().setIntercept(intercept)
    linReg.optimizer.setNumIterations(iterations).setStepSize(step)
    val linear_model = linReg.run(train)

    val true_vs_predicted = test.map(p => (p.label, linear_model.predict(p.features)))
    val rmsle = Math.sqrt(true_vs_predicted.map{ case(t, p) => Util.squaredLogError(t, p)}.mean())
    return rmsle
  }

}
