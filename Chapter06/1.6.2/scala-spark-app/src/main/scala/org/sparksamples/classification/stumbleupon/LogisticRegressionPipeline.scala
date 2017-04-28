package org.sparksamples.classification.stumbleupon

import org.apache.log4j.Logger
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.DataFrame

/**
  * Created by manpreet.singh on 01/05/16.
  */
object LogisticRegressionPipeline {
  @transient lazy val logger = Logger.getLogger(getClass.getName)

  def logisticRegressionPipeline(vectorAssembler: VectorAssembler, dataFrame: DataFrame) = {
    val lr = new LogisticRegression()

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.25, 0.5, 0.75, 1.0))
      .build()

    val pipeline = new Pipeline().setStages(Array(vectorAssembler, lr))

    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(pipeline)
      .setEvaluator(new RegressionEvaluator)
      .setEstimatorParamMaps(paramGrid)
      // 80% of the data will be used for training and the remaining 20% for validation.
      .setTrainRatio(0.8)

    val Array(training, test) = dataFrame.randomSplit(Array(0.8, 0.2), seed = 12345)
    //val model = trainValidationSplit.fit(training)
    val model = trainValidationSplit.fit(dataFrame)

    //val holdout = model.transform(test).select("prediction","label")
    val holdout = model.transform(dataFrame).select("prediction","label")

    // have to do a type conversion for RegressionMetrics
    val rm = new RegressionMetrics(holdout.rdd.map(x => (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))

    logger.info("Test Metrics")
    logger.info("Test Explained Variance:")
    logger.info(rm.explainedVariance)
    logger.info("Test R^2 Coef:")
    logger.info(rm.r2)
    logger.info("Test MSE:")
    logger.info(rm.meanSquaredError)
    logger.info("Test RMSE:")
    logger.info(rm.rootMeanSquaredError)

    val totalPoints = dataFrame.count()
    val lrTotalCorrect = holdout.rdd.map(x => if (x(0).asInstanceOf[Double] == x(1).asInstanceOf[Double]) 1 else 0).sum()
    val accuracy = lrTotalCorrect/totalPoints
    println("Accuracy of LogisticRegression is: ", accuracy)

    holdout.rdd.map(x => x(0).asInstanceOf[Double]).repartition(1).saveAsTextFile("/home/ubuntu/work/ml-resources/spark-ml/results/LR.xls")
    holdout.rdd.map(x => x(1).asInstanceOf[Double]).repartition(1).saveAsTextFile("/home/ubuntu/work/ml-resources/spark-ml/results/Actual.xls")

    savePredictions(holdout, dataFrame, rm, "/home/ubuntu/work/ml-resources/spark-ml/results/LogisticRegression.csv")
  }

  def savePredictions(predictions:DataFrame, testRaw:DataFrame, regressionMetrics: RegressionMetrics, filePath:String) = {
    println("Mean Squared Error:", regressionMetrics.meanSquaredError)
    println("Root Mean Squared Error:", regressionMetrics.rootMeanSquaredError)

    predictions
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save(filePath)
  }
}
