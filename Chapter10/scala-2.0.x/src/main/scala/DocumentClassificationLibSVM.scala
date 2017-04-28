/**
  * Created by ubuntu on 12/28/16.
  */

package org.apache.spark.examples.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import org.apache.spark.sql.SparkSession

object DocumentClassificationLibSVM {
  def main(args: Array[String]): Unit = {

    val spConfig = (new SparkConf).setMaster("local").setAppName("SparkApp")
    val spark = SparkSession
      .builder()
      .appName("SparkRatingData").config(spConfig)
      .getOrCreate()

    val data = spark.read.format("libsvm").load("./output/20news-by-date-train-libsvm/part-combined")

    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3), seed = 1L)

    // Train a NaiveBayes model.
    val model = new NaiveBayes()
      .fit(trainingData)
    val predictions = model.transform(testData)
    predictions.show()

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Test set accuracy = " + accuracy)
    spark.stop()
  }
}
