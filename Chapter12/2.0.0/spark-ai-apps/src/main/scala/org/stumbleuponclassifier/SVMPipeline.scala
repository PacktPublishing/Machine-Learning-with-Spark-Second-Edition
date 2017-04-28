package org.stumbleuponclassifier

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * Created by manpreet.singh on 01/05/16.
  */
object SVMPipeline {
  @transient lazy val logger = Logger.getLogger(getClass.getName)

  def svmPipeline(sc: SparkContext) = {
    val records = sc.textFile("/home/ubuntu/work/ml-resources/spark-ml/train_noheader.tsv").map(line => line.split("\t"))

    val data = records.map { r =>
      val trimmed = r.map(_.replaceAll("\"", ""))
      val label = trimmed(r.size - 1).toInt
      val features = trimmed.slice(4, r.size - 1).map(d => if (d == "?") 0.0 else d.toDouble)
      LabeledPoint(label, Vectors.dense(features))
    }

    // params for SVM
    val numIterations = 10

    // Run training algorithm to build the model
    val svmModel = SVMWithSGD.train(data, numIterations)

    // Clear the default threshold.
    svmModel.clearThreshold()

    val svmTotalCorrect = data.map { point =>
      if(svmModel.predict(point.features) == point.label) 1 else 0
    }.sum()

    // calculate accuracy
    val svmAccuracy = svmTotalCorrect / data.count()
    println(svmAccuracy)
  }

}
