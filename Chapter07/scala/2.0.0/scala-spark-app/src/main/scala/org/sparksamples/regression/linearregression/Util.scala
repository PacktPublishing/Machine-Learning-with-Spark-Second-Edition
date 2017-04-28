package org.sparksamples

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map

/**
  * Created by rajdeep dua on 4/15/16.
  */
object Util {
  val PATH= "./src/main/scala/org/sparksamples/regression/dataset/BikeSharing/hour_noheader.csv"
  val spConfig = (new SparkConf).setMaster("local[1]").setAppName("SparkApp").
    set("spark.driver.allowMultipleContexts", "true")
  val sc = new SparkContext(spConfig)

  def getRecords() : (RDD[Array[String]],Long) ={
    //val sc = new SparkContext("local[2]", "First Spark App")
    //val conf = new SparkConf().setMaster("local").setAppName("GradientBoostedTreesRegressionApp")
    //sc = new SparkContext(conf)
    // we take the raw data in CSV format and convert it into a set of records
    // of the form (user, product, price)
    val rawData = sc.textFile(PATH)
    val numData = rawData.count()

    val records = rawData.map(line => line.split(","))
    return (records, numData)
  }

  def extractFeatures(record : Array[String], cat_len: Int,
                      mappings:scala.collection.immutable.List[scala.collection.Map[String,Long]]): Vector ={
    var cat_vec = Vectors.zeros(cat_len)
    val cat_array = cat_vec.toArray
    var i = 0
    var step = 0
    for(field <- 2 until 10){
      val m = mappings(i)
      var idx = 0L
      try {
        if (m.keySet.exists(_ == field.toString)) {
          idx = m(field.toString)
        }
      }catch {
        case e: Exception => print(e)
      }
      cat_array(idx.toInt + step) = 1
      i +=1
      step = step + m.size
    }
    cat_vec = Vectors.dense(cat_array)

    val record_2 = record.slice(10,14)

    val record_3 = Array.fill(record_2.length){0.0}
    for( i<- 0 until record_2.length){
      record_3(i) = record_2(i).toDouble
    }
    val total_len = cat_array.length + record_2.length
    val record_4 = Array.fill(total_len){0.0}

    for( i<- 0 until cat_array.length){
      record_4(i) = cat_array(i)
    }
    for( i<- 0 until record_2.length){
      record_4(11 + i) = record_2(i).toDouble
    }

    val final_vc = Vectors.dense(record_4)

    return final_vc
  }

  def extract_features_dt(record : Array[String]): Vector = {
    val cat_len = record.length
    var cat_vec = Vectors.zeros(cat_len)
    var cat_array = Array[Double](cat_len)
    var idx = 0
    val record_2 = record.slice(2,14)


    return Vectors.dense(record_2.map(x => x.toDouble))

  }

  def extractSumFeature(record : Array[String], cat_len: Int,
                      mappings:scala.collection.immutable.List[scala.collection.Map[String,Long]]): Double ={
    //var cat_vec = Vectors.zeros(cat_len)
    //val cat_array = cat_vec.toArray
    val x = extractFeatures(record, cat_len, mappings)
    var sum = 0.0
    for(y <- 0 until 14){

        sum = sum + x(y).toDouble
    }
    return sum.toDouble
  }

  def extractAvgFeature2(record : Array[String]): Double ={
    //var cat_vec = Vectors.zeros(cat_len)
    //val cat_array = cat_vec.toArray
    val x = record
    var sum = 0.0
    for(y <- 0 until 14){

      sum = sum + x(y).toDouble
    }
    return sum.toDouble
  }

  def extractTwoFeatures(record : Array[String], cat_len: Int,
                        mappings:scala.collection.immutable.List[scala.collection.Map[String,Long]]): String ={

    var sumX = 0L
    var i = 0
    for(field <- 2 until 5){
      val m = mappings(i)
      var idx = 0L
      try {
        if (m.keySet.exists(_ == field.toString)) {
          idx = m(field.toString).toLong
          sumX = sumX + idx
        }
      }catch {
        case e: Exception => print(e)
      }
      i +=1
    }
    var j = 0
    val x = sumX/i
    var sumY = 0L
    for(field <- 6 until 9){
      val m = mappings(i)
      var idx = 0L
      try {
        if (m.keySet.exists(_ == field.toString)) {
          idx = m(field.toString).toLong
          sumY = sumY + idx
        }
      }catch {
        case e: Exception => print(e)
      }
      j +=1

    }
    val y = sumY/j

    return x + ", "  + y
  }

  def extractLabel(r: Array[String]): Double ={
    //print(r( r.length -1))
    return r( r.length -1).toDouble
  }

  def squaredError(actual:Double, pred : Double) : Double = {
    return Math.pow( (pred - actual), 2.0)
  }

  def absError(actual:Double, pred : Double) : Double = {
    return Math.abs( (pred - actual))
  }

  def squaredLogError(actual:Double, pred : Double) : Double = {
    return Math.pow( (Math.log(pred +1) - Math.log(actual +1)), 2.0)
  }

  def get_mapping(rdd :RDD[Array[String]], idx: Int) : Map[String, Long] = {
    return rdd.map( fields=> fields(idx)).distinct().zipWithIndex().collectAsMap()
  }

  def calculatePrintMetrics(true_vs_predicted: RDD[(Double, Double)], algo: String) = {
    val mse = true_vs_predicted.map{ case(t, p) => Util.squaredError(t, p)}.mean()
    val mae = true_vs_predicted.map{ case(t, p) => Util.absError(t, p)}.mean()
    val rmsle = Math.sqrt(true_vs_predicted.map{ case(t, p) => Util.squaredLogError(t, p)}.mean())

    println(algo + " - Mean Squared Error: "  + mse)
    println(algo + " - Mean Absolute Error: " + mae)
    println(algo + " - Root Mean Squared Log Error:" + rmsle)
  }

}
